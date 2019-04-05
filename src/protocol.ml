open Core

module Array = struct
  include Array

  (* [init] does not promise to do so in ascending index order. I think nobody would ever
     imagine changing this, and it would show up in our tests, but it's cheap to be
     explicit. *)
  let init_ascending len ~f =
    match len with
    | 0 -> [||]
    | len ->
      let res = create ~len (f ()) in
      for i = 1 to len - 1 do
        unsafe_set res i (f ())
      done;
      res
end

module Frontend = struct
  module Shared = struct
    let validate_null_terminated_exn ~field_name str =
      if String.mem str '\x00'
      then raise_s [%message "String may not contain nulls" field_name str]

    let fill_null_terminated iobuf str =
      Iobuf.Fill.stringo iobuf str;
      Iobuf.Fill.char iobuf '\x00'
  end

  module StartupMessage = struct
    let message_type_char = None

    type t =
      { user : string
      ; database : string
      }

    let validate_exn { user; database } =
      Shared.validate_null_terminated_exn ~field_name:"user" user;
      Shared.validate_null_terminated_exn ~field_name:"database" database

    let payload_length { user; database } =
      2
      + 2
      + 4 + 1
      + String.length user + 1
      + 8 + 1
      + String.length database + 1
      + 1

    let fill { user; database } iobuf =
      Iobuf.Fill.int16_be_trunc iobuf 3; (* major *)
      Iobuf.Fill.int16_be_trunc iobuf 0; (* minor *)
      Iobuf.Fill.stringo iobuf "user\x00";
      Shared.fill_null_terminated iobuf user;
      Iobuf.Fill.stringo iobuf "database\x00";
      Shared.fill_null_terminated iobuf database;
      Iobuf.Fill.char iobuf '\x00'
  end

  module PasswordMessage = struct
    let message_type_char = Some 'p'

    type t =
      | Cleartext_or_md5_hex of string
      | Gss_binary_blob of string

    let validate_exn =
      function
      | Cleartext_or_md5_hex password ->
        Shared.validate_null_terminated_exn ~field_name:"password" password
      | Gss_binary_blob _ -> ()

    let payload_length =
      function
      | Cleartext_or_md5_hex password -> String.length password + 1
      | Gss_binary_blob blob -> String.length blob

    let fill t iobuf =
      match t with
      | Cleartext_or_md5_hex password -> Shared.fill_null_terminated iobuf password
      | Gss_binary_blob blob -> Iobuf.Fill.stringo iobuf blob
  end

  module Parse = struct
    let message_type_char = Some 'P'

    type t =
      { destination : Types.Statement_name.t
      ; query : string
      }

    let validate_exn t =
      Shared.validate_null_terminated_exn t.query ~field_name:"query"

    let payload_length t =
      + String.length (Types.Statement_name.to_string t.destination) + 1
      + String.length t.query + 1
      + 2

    let fill t iobuf =
      Shared.fill_null_terminated iobuf (Types.Statement_name.to_string t.destination);
      Shared.fill_null_terminated iobuf t.query;
      Iobuf.Fill.int16_be_trunc iobuf 0 (* zero parameter types *)
  end

  module Bind = struct
    let message_type_char = Some 'B'

    type t =
      { destination : Types.Portal_name.t
      ; statement : Types.Statement_name.t
      ; parameters : string option array
      }

    let validate_exn (_ : t) = ()

    let payload_length t =
      let parameter_length =
        function
        | None -> 0
        | Some s -> String.length s
      in
      + String.length (Types.Portal_name.to_string t.destination) + 1
      + String.length (Types.Statement_name.to_string t.statement) + 1
      + 2 (* # parameter format codes = 1 *)
      + 2 (* single parameter format code *)
      + 2 (* # parameters *)
      + 4 * Array.length t.parameters (* parameter sizes *)
      + Array.sum (module Int) t.parameters ~f:parameter_length
      + 2 (* # result format codes = 1 *)
      + 2 (* single result format code *)

    let fill t iobuf =
      Shared.fill_null_terminated iobuf (Types.Portal_name.to_string t.destination);
      Shared.fill_null_terminated iobuf (Types.Statement_name.to_string t.statement);
      Iobuf.Fill.int16_be_trunc iobuf 1; (* 1 parameter format code *)
      Iobuf.Fill.int16_be_trunc iobuf 0; (* all parameters are text *)
      let num_parameters = Array.length t.parameters in
      Iobuf.Fill.int16_be_trunc iobuf num_parameters;
      for idx = 0 to num_parameters - 1 do
        match t.parameters.(idx) with
        | None ->
          Iobuf.Fill.int32_be_trunc iobuf (-1)
        | Some str ->
          Iobuf.Fill.int32_be_trunc iobuf (String.length str);
          Iobuf.Fill.stringo iobuf str
      done;
      Iobuf.Fill.int16_be_trunc iobuf 1; (* 1 result format code *)
      Iobuf.Fill.int16_be_trunc iobuf 0; (* all results are text *)
  end

  module Execute = struct
    let message_type_char = Some 'E'

    type num_rows =
      | Unlimited
      | Limit of int

    type t =
      { portal : Types.Portal_name.t
      ; limit : num_rows
      }

    let validate_exn t =
      match t.limit with
      | Unlimited -> ()
      | Limit n ->
        (if n <= 0 then failwith "When provided, num rows limit must be positive")

    let payload_length t =
      + String.length (Types.Portal_name.to_string t.portal) + 1
      + 4

    let fill t iobuf =
      Shared.fill_null_terminated iobuf (Types.Portal_name.to_string t.portal);
      let limit =
        match t.limit with
        | Unlimited -> 0
        | Limit n -> n
      in
      Iobuf.Fill.int32_be_trunc iobuf limit
  end

  module Statement_or_portal_action = struct
    type t =
      | Statement of Types.Statement_name.t
      | Portal    of Types.Portal_name.t

    let validate_exn (_t : t) = ()

    let payload_length t =
      let str =
        match t with
        | Statement s -> Types.Statement_name.to_string s
        | Portal s -> Types.Portal_name.to_string s
      in
      1 + String.length str + 1

    let fill t iobuf =
      match t with
      | Statement s ->
        Iobuf.Fill.char iobuf 'S';
        Shared.fill_null_terminated iobuf (Types.Statement_name.to_string s)
      | Portal p ->
        Iobuf.Fill.char iobuf 'P';
        Shared.fill_null_terminated iobuf (Types.Portal_name.to_string p)
  end

  module Describe = struct
    let message_type_char = Some 'D'
    include Statement_or_portal_action
  end

  module Close = struct
    let message_type_char = Some 'C'
    include Statement_or_portal_action
  end

  module CopyFail = struct
    let message_type_char = Some 'f'

    type t =
      { reason : string
      }

    let validate_exn t =
      Shared.validate_null_terminated_exn t.reason ~field_name:"reason"

    let payload_length t =
      String.length t.reason + 1

    let fill t iobuf =
      Shared.fill_null_terminated iobuf t.reason;
  end

  module CopyData = struct
    let message_type_char = Some 'd'

    type t = string

    let validate_exn (_ : t) = ()
    let payload_length t = String.length t
    let fill t iobuf = Iobuf.Fill.stringo iobuf t
  end

  module No_arg : sig
    val flush : string
    val sync  : string
    val copy_done : string
    val terminate : string
  end = struct
    let gen ~constructor =
      let tmp = Iobuf.create ~len:5 in
      Iobuf.Poke.char tmp ~pos:0 constructor;
      Iobuf.Poke.int32_be_trunc tmp ~pos:1 4;
      Iobuf.to_string tmp

    let flush     = gen ~constructor:'H'
    let sync      = gen ~constructor:'S'
    let copy_done = gen ~constructor:'c'
    let terminate = gen ~constructor:'X'
  end

  include No_arg

  module Writer = struct
    open Async

    module type Message_type = sig
      val message_type_char : char option
      type t
      val validate_exn : t -> unit
      val payload_length : t -> int
      val fill : t -> (read_write, Iobuf.seek) Iobuf.t -> unit
    end

    type 'a with_computed_length = { payload_length : int; value : 'a }

    let write_message (type a) (module M : Message_type with type t = a) =
      let full_length { payload_length; _ } =
        match M.message_type_char with
        | None -> payload_length + 4
        | Some _ -> payload_length + 5
      in
      let blit_to_bigstring with_computed_length bigstring ~pos =
        let iobuf =
          Iobuf.of_bigstring
            bigstring
            ~pos
            ~len:(full_length with_computed_length)
        in
        (match M.message_type_char with
         | None -> ()
         | Some c -> Iobuf.Fill.char iobuf c);
        let { payload_length; value } = with_computed_length in
        Iobuf.Fill.int32_be_trunc iobuf (payload_length + 4);
        M.fill value iobuf;
        (match Iobuf.is_empty iobuf with
         | true -> ()
         | false -> failwith "postgres message filler lied about length")
      in
      Staged.stage (fun writer value ->
        M.validate_exn value;
        let payload_length = M.payload_length value in
        Writer.write_gen_whole
          writer
          { payload_length; value }
          ~length:full_length
          ~blit_to_bigstring
      )

    let startup_message  = Staged.unstage (write_message (module StartupMessage))
    let password_message = Staged.unstage (write_message (module PasswordMessage))
    let parse            = Staged.unstage (write_message (module Parse))
    let bind             = Staged.unstage (write_message (module Bind))
    let close            = Staged.unstage (write_message (module Close))
    let describe         = Staged.unstage (write_message (module Describe))
    let execute          = Staged.unstage (write_message (module Execute))
    let copy_fail        = Staged.unstage (write_message (module CopyFail))
    let copy_data        = Staged.unstage (write_message (module CopyData))

    let flush     writer = Writer.write writer No_arg.flush
    let sync      writer = Writer.write writer No_arg.sync
    let copy_done writer = Writer.write writer No_arg.copy_done
    let terminate writer = Writer.write writer No_arg.terminate
  end
end

module Backend = struct
  type constructor =
    | AuthenticationRequest
    | BackendKeyData
    | BindComplete
    | CloseComplete
    | CommandComplete
    | CopyData
    | CopyDone
    | CopyInResponse
    | CopyOutResponse
    | CopyBothResponse
    | DataRow
    | EmptyQueryResponse
    | ErrorResponse
    | FunctionCallResponse
    | NoData
    | NoticeResponse
    | NotificationResponse
    | ParameterDescription
    | ParameterStatus
    | ParseComplete
    | PortalSuspended
    | ReadyForQuery
    | RowDescription
  [@@deriving sexp, compare]

  let constructor_of_char =
    function
    | 'R' -> Ok AuthenticationRequest
    | 'K' -> Ok BackendKeyData
    | '2' -> Ok BindComplete
    | '3' -> Ok CloseComplete
    | 'C' -> Ok CommandComplete
    | 'd' -> Ok CopyData
    | 'c' -> Ok CopyDone
    | 'G' -> Ok CopyInResponse
    | 'H' -> Ok CopyOutResponse
    | 'W' -> Ok CopyBothResponse
    | 'D' -> Ok DataRow
    | 'I' -> Ok EmptyQueryResponse
    | 'E' -> Ok ErrorResponse
    | 'V' -> Ok FunctionCallResponse
    | 'n' -> Ok NoData
    | 'N' -> Ok NoticeResponse
    | 'A' -> Ok NotificationResponse
    | 't' -> Ok ParameterDescription
    | 'S' -> Ok ParameterStatus
    | '1' -> Ok ParseComplete
    | 's' -> Ok PortalSuspended
    | 'Z' -> Ok ReadyForQuery
    | 'T' -> Ok RowDescription
    | other -> Error (`Unknown_message_type other)

  let focus_on_message iobuf =
    let iobuf_length = Iobuf.length iobuf in
    if iobuf_length < 5
    then Error `Too_short
    else (
      let message_length =
        (Iobuf.Peek.int32_be iobuf ~pos:1) + 1
      in
      if iobuf_length < message_length
      then Error `Too_short
      else (
        let char = Iobuf.Peek.char iobuf ~pos:0 in
        match constructor_of_char char with
        | Error _ as err -> err
        | Ok _ as ok ->
          Iobuf.resize iobuf ~len:message_length;
          Iobuf.advance iobuf 5;
          ok))

  module Shared = struct
    let find_null_exn iobuf =
      let rec loop ~iobuf ~length ~pos =
        if Iobuf.Peek.char iobuf ~pos = '\x00'
        then pos
        else if pos > length - 1
        then failwith "find_null_exn could not find \\x00"
        else loop ~iobuf ~length ~pos:(pos + 1)
      in
      loop ~iobuf ~length:(Iobuf.length iobuf) ~pos:0

    let consume_cstring_exn iobuf =
      let len = find_null_exn iobuf in
      let res = Iobuf.Consume.string iobuf ~len:len ~str_pos:0 in
      let zero = Iobuf.Consume.char iobuf in
      assert (zero = '\x00');
      res
  end

  module Error_or_Notice = struct
    let field_name =
      function
      | 'S' -> "severity"
      | 'V' -> "severity-non-localised"
      | 'C' -> "code"
      | 'M' -> "message"
      | 'D' -> "detail"
      | 'H' -> "hint"
      | 'P' -> "position"
      | 'p' -> "internal_position"
      | 'q' -> "internal_query"
      | 'W' -> "where"
      | 's' -> "schema"
      | 't' -> "table"
      | 'c' -> "column"
      | 'd' -> "data_type"
      | 'n' -> "constraint"
      | 'F' -> "file"
      | 'L' -> "line"
      | 'R' -> "routine"
      | other ->
        (* the spec requires that we silently ignore unrecognised codes. *)
        sprintf "unknown-%c" other

    let consume_exn iobuf =
      let rec loop ~iobuf ~fields_rev =
        match Iobuf.Consume.char iobuf with
        | '\x00' ->
          Info.create_s [%sexp (List.rev fields_rev : (string * string) list)]
        | other ->
          let tok = field_name other in
          let value = Shared.consume_cstring_exn iobuf in
          loop ~iobuf ~fields_rev:((tok, value) :: fields_rev)
      in
      loop ~iobuf ~fields_rev:[]
  end

  module ErrorResponse = struct
    type t = Error.t

    let consume iobuf =
      match Error_or_Notice.consume_exn iobuf with
      | exception exn -> error_s [%message "Failed to parse ErrorResponse" (exn : Exn.t)]
      | info -> Ok (Error.of_info info)
  end

  module NoticeResponse = struct
    type t = Info.t

    let consume iobuf =
      match Error_or_Notice.consume_exn iobuf with
      | exception exn -> error_s [%message "Failed to parse NoticeResponse" (exn : Exn.t)]
      | info -> Ok info
  end

  module AuthenticationRequest = struct
    type t =
      | Ok
      | KerberosV5
      | CleartextPassword
      | MD5Password of { salt : string }
      | SCMCredential
      | GSS
      | SSPI
      | GSSContinue of { data : string }
    [@@deriving sexp]

    let consume_exn iobuf =
      match Iobuf.Consume.int32_be iobuf with
      | 0 -> Ok
      | 2 -> KerberosV5
      | 3 -> CleartextPassword
      | 5 ->
        let salt = Iobuf.Consume.stringo ~len:4 iobuf in
        MD5Password { salt }
      | 6 -> SCMCredential
      | 7 -> GSS
      | 9 -> SSPI
      | 8 ->
        let data = Iobuf.Consume.stringo iobuf in
        GSSContinue { data }
      | other ->
        raise_s [%message "AuthenticationRequest unrecognised type" (other : int)]

    let consume iobuf =
      match consume_exn iobuf with
      | exception exn -> error_s [%message "Failed to parse AuthenticationRequest" (exn : Exn.t)]
      | t -> Result.Ok t
  end

  module ParameterStatus = struct
    type t =
      { key : string
      ; data : string
      }

    let consume_exn iobuf =
      let key = Shared.consume_cstring_exn iobuf in
      let data = Shared.consume_cstring_exn iobuf in
      { key; data }

    let consume iobuf =
      match consume_exn iobuf with
      | exception exn -> error_s [%message "Failed to parse ParameterStatus" (exn : Exn.t)]
      | t -> Ok t
  end

  module BackendKeyData = struct
    type t = Types.backend_key

    let consume_exn iobuf =
      let pid = Pid.of_int (Iobuf.Consume.int32_be iobuf) in
      let secret = Iobuf.Consume.int32_be iobuf in
      { Types. pid; secret }

    let consume iobuf =
      match consume_exn iobuf with
      | exception exn -> error_s [%message "Failed to parse BackendKeyData" (exn : Exn.t)]
      | t -> Ok t
  end

  module NotificationResponse = struct
    type t =
      { pid : Pid.t
      ; channel : Types.Notification_channel.t
      ; payload : string
      }

    let consume_exn iobuf =
      let pid = Pid.of_int (Iobuf.Consume.int32_be iobuf) in
      let channel =
        Shared.consume_cstring_exn iobuf
        |> Types.Notification_channel.of_string
      in
      let payload = Shared.consume_cstring_exn iobuf in
      { pid; channel; payload }

    let consume iobuf =
      match consume_exn iobuf with
      | exception exn -> error_s [%message "Failed to parse NotificationResponse" (exn : Exn.t)]
      | t -> Ok t
  end

  module ReadyForQuery = struct
    type t =
      | Idle
      | In_transaction
      | In_failed_transaction
    [@@deriving sexp_of]

    let consume_exn iobuf =
      match Iobuf.Consume.char iobuf with
      | 'I' -> Idle
      | 'T' -> In_transaction
      | 'E' -> In_failed_transaction
      | other -> raise_s [%message "unrecognised backend status char in ReadyForQuery" (other : char)]

    let consume iobuf =
      match consume_exn iobuf with
      | exception exn -> error_s [%message "Failed to parse ReadyForQuery" (exn : Exn.t)]
      | t -> Ok t
  end

  module ParseComplete = struct
    let consume (_ : _ Iobuf.t) = ()
  end

  module BindComplete = struct
    let consume (_ : _ Iobuf.t) = ()
  end

  module NoData = struct
    let consume (_ : _ Iobuf.t) = ()
  end

  module EmptyQueryResponse = struct
    let consume (_ : _ Iobuf.t) = ()
  end

  module CopyDone = struct
    let consume (_ : _ Iobuf.t) = ()
  end

  module RowDescription = struct
    type column =
      { name : string
      ; format : [ `Text ]
      }

    type t = column array

    let consume_exn iobuf =
      let num_fields = Iobuf.Consume.int16_be iobuf in
      Array.init_ascending num_fields ~f:(fun () ->
        let name = Shared.consume_cstring_exn iobuf in
        let skip =
          4 (* table *)
          + 2 (* column *)
          + 4 (* type oid *)
          + 4 (* type modifier *)
          + 2 (* length-or-variable *)
        in
        Iobuf.advance iobuf skip;
        let format =
          match Iobuf.Consume.int16_be iobuf with
          | 0 -> `Text
          | 1 -> failwith "RowDescription format=Binary?"
          | i -> failwithf "RowDescription: bad format %i" i ()
        in
        { name; format }
      )

    let consume iobuf =
      match consume_exn iobuf with
      | exception exn -> error_s [%message "Failed to parse RowDescription" (exn : Exn.t)]
      | cols -> Ok cols
  end

  module DataRow = struct
    type t = string option array

    let consume_exn iobuf =
      let num_fields = Iobuf.Consume.int16_be iobuf in
      Array.init_ascending num_fields ~f:(fun () ->
        match Iobuf.Consume.int32_be iobuf with
        | -1 -> None
        | len -> Some (Iobuf.Consume.stringo iobuf ~len)
      )

    let consume iobuf =
      match consume_exn iobuf with
      | exception exn -> error_s [%message "Failed to parse DataRow" (exn : Exn.t)]
      | t -> Ok t

    let skip iobuf = Iobuf.advance iobuf (Iobuf.length iobuf)
  end

  module type CopyResponse = sig
    type column =
      { name : string
      ; format : [ `Text | `Binary ]
      }

    type t =
      { overall_format : [ `Text | `Binary ]
      ; num_columns : int
      ; column_formats : [ `Text | `Binary ] array
      }

    val consume : ([> read ], Iobuf.seek) Iobuf.t -> t Or_error.t
  end

  module CopyResponse (A : sig val name : string end) : CopyResponse = struct
    type column =
      { name : string
      ; format : [ `Text | `Binary ]
      }

    type t =
      { overall_format : [ `Text | `Binary ]
      ; num_columns : int
      ; column_formats : [ `Text | `Binary ] array
      }

    let consume_exn iobuf =
      let overall_format =
        match Iobuf.Consume.int8 iobuf with
        | 0 -> `Text
        | 1 -> `Binary
        | i -> failwithf "%s: bad overall format: %i" A.name i ()
      in
      let num_columns = Iobuf.Consume.int16_be iobuf in
      let column_formats =
        Array.init_ascending num_columns ~f:(fun () ->
          match Iobuf.Consume.int16_be iobuf with
          | 0 -> `Text
          | 1 -> `Binary
          | i -> failwithf "%s: bad format %i" A.name i ()
        )
      in
      { overall_format; num_columns; column_formats }

    let consume iobuf =
      match consume_exn iobuf with
      | cols -> Ok cols
      | exception exn ->
        let s = sprintf "Failed to parse %s" A.name in
        error_s [%message s (exn : Exn.t)]
  end

  module CopyInResponse = CopyResponse(struct let name = "CopyInResponse" end)
  module CopyOutResponse = CopyResponse(struct let name = "CopyOutResponse" end)

  module CopyData = struct
    (* After [focus_on_message] seeks over the type and length, 'CopyData'
       messages are simply just the payload bytes. *)
    let skip iobuf = Iobuf.advance iobuf (Iobuf.length iobuf)
  end

  module CommandComplete = struct
    type t = string

    let consume_exn = Shared.consume_cstring_exn

    let consume iobuf =
      match consume_exn iobuf with
      | exception exn -> error_s [%message "Failed to parse CommandComplete" (exn : Exn.t)]
      | t -> Ok t
  end
end
