open! Core
open! Async
open! Import

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
  ;;
end

module Iarray = struct
  include Iarray

  (* [init] does not promise to do so in ascending index order. I think nobody would ever
     imagine changing this, and it would show up in our tests, but it's cheap to be
     explicit. *)
  let init_ascending len ~f =
    match len with
    | 0 -> Iarray.empty
    | len ->
      let res = Array.create ~len (f ()) in
      for i = 1 to len - 1 do
        Array.unsafe_set res i (f ())
      done;
      Iarray.unsafe_of_array__promise_no_mutation res
  ;;
end

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
[@@deriving compare, equal, sexp]

type focus_on_message_error =
  | Unknown_message_type of char
  | Iobuf_too_short_for_header
  | Iobuf_too_short_for_message of { message_length : int }
  | Nonsense_message_length of int

let constructor_of_char = function
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
  | other -> Error (Unknown_message_type other)
;;

let focus_on_message iobuf =
  let iobuf_length = Iobuf.length iobuf in
  if iobuf_length < 5
  then Error Iobuf_too_short_for_header
  else (
    let message_length = Iobuf.Peek.int32_be iobuf ~pos:1 + 1 in
    if iobuf_length < message_length
    then Error (Iobuf_too_short_for_message { message_length })
    else if message_length < 5
    then Error (Nonsense_message_length message_length)
    else (
      let char = Iobuf.Peek.char iobuf ~pos:0 in
      match constructor_of_char char with
      | Error _ as err -> err
      | Ok _ as ok ->
        Iobuf.resize iobuf ~len:message_length;
        Iobuf.advance iobuf 5;
        ok))
;;

module Error_or_notice_field = struct
  type other = char [@@deriving equal, sexp_of]

  type t =
    | Severity
    | Severity_non_localised
    | Code
    | Message
    | Detail
    | Hint
    | Position
    | Internal_position
    | Internal_query
    | Where
    | Schema
    | Table
    | Column
    | Data_type
    | Constraint
    | File
    | Line
    | Routine
    | Other of other
  [@@deriving equal, sexp_of]

  let of_protocol : char -> t = function
    | 'S' -> Severity
    | 'V' -> Severity_non_localised
    | 'C' -> Code
    | 'M' -> Message
    | 'D' -> Detail
    | 'H' -> Hint
    | 'P' -> Position
    | 'p' -> Internal_position
    | 'q' -> Internal_query
    | 'W' -> Where
    | 's' -> Schema
    | 't' -> Table
    | 'c' -> Column
    | 'd' -> Data_type
    | 'n' -> Constraint
    | 'F' -> File
    | 'L' -> Line
    | 'R' -> Routine
    | other ->
      (* the spec requires that we silently ignore unrecognised codes. *)
      Other other
  ;;

  let to_protocol = function
    | Severity -> 'S'
    | Severity_non_localised -> 'V'
    | Code -> 'C'
    | Message -> 'M'
    | Detail -> 'D'
    | Hint -> 'H'
    | Position -> 'P'
    | Internal_position -> 'p'
    | Internal_query -> 'q'
    | Where -> 'W'
    | Schema -> 's'
    | Table -> 't'
    | Column -> 'c'
    | Data_type -> 'd'
    | Constraint -> 'n'
    | File -> 'F'
    | Line -> 'L'
    | Routine -> 'R'
    | Other c -> c
  ;;
end

module Error_or_Notice = struct
  type t =
    { error_code : string
    ; all_fields : (Error_or_notice_field.t * string) list
    }
  [@@deriving sexp_of]

  let consume_exn iobuf =
    let rec loop ~iobuf ~fields_rev =
      match Iobuf.Consume.char iobuf with
      | '\x00' ->
        let all_fields = List.rev fields_rev in
        let error_code =
          List.Assoc.find all_fields Code ~equal:[%equal: Error_or_notice_field.t]
          |> Option.value_exn ~message:"code field is mandatory"
        in
        { all_fields; error_code }
      | other ->
        let tok = Error_or_notice_field.of_protocol other in
        let value = Shared.consume_cstring_exn iobuf in
        loop ~iobuf ~fields_rev:((tok, value) :: fields_rev)
    in
    loop ~iobuf ~fields_rev:[]
  ;;

  let payload_length { error_code; all_fields } =
    1
    + String.length error_code
    + 1
    + List.fold all_fields ~init:0 ~f:(fun acc ((_ : Error_or_notice_field.t), data) ->
      (* 1 for code, 1 for null term *)
      1 + String.length data + 1 + acc)
    (* zero terminates *)
    + 1
  ;;
end

module ErrorResponse = struct
  include Error_or_Notice

  let consume iobuf =
    match Error_or_Notice.consume_exn iobuf with
    | exception exn -> error_s [%message "Failed to parse ErrorResponse" (exn : Exn.t)]
    | t -> Ok t
  ;;

  let message_type_char = Some 'E'
  let validate_exn (_ : t) = ()

  let fill { error_code; all_fields } iobuf =
    Iobuf.Fill.char iobuf (Error_or_notice_field.to_protocol Code);
    Shared.fill_null_terminated iobuf error_code;
    List.iter all_fields ~f:(fun (field, data) ->
      Iobuf.Fill.char iobuf (Error_or_notice_field.to_protocol field);
      Shared.fill_null_terminated iobuf data);
    Iobuf.Fill.char iobuf '\x00'
  ;;
end

module NoticeResponse = struct
  include Error_or_Notice

  let message_type_char = Some 'N'
  let validate_exn (_ : t) = ()

  let consume iobuf =
    match Error_or_Notice.consume_exn iobuf with
    | exception exn -> error_s [%message "Failed to parse NoticeResponse" (exn : Exn.t)]
    | t -> Ok t
  ;;

  let fill { error_code; all_fields } iobuf =
    Iobuf.Fill.char iobuf (Error_or_notice_field.to_protocol Code);
    Shared.fill_null_terminated iobuf error_code;
    List.iter all_fields ~f:(fun (field, data) ->
      Iobuf.Fill.char iobuf (Error_or_notice_field.to_protocol field);
      Shared.fill_null_terminated iobuf data);
    Iobuf.Fill.char iobuf '\x00'
  ;;
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
    | other -> raise_s [%message "AuthenticationRequest unrecognised type" (other : int)]
  ;;

  let consume iobuf =
    match consume_exn iobuf with
    | exception exn ->
      error_s [%message "Failed to parse AuthenticationRequest" (exn : Exn.t)]
    | t -> Result.Ok t
  ;;

  let message_type_char = Some 'R'
  let validate_exn (_ : t) = ()

  let payload_length t =
    let var_length =
      match t with
      | Ok | KerberosV5 | CleartextPassword | SCMCredential | GSS | SSPI -> 0
      | MD5Password { salt } -> String.length salt
      | GSSContinue { data } -> String.length data
    in
    4 + var_length
  ;;

  let fill t iobuf =
    let code =
      match t with
      | Ok -> 0
      | KerberosV5 -> 2
      | CleartextPassword -> 3
      | MD5Password { salt = _ } -> 5
      | SCMCredential -> 6
      | GSS -> 7
      | SSPI -> 9
      | GSSContinue { data = _ } -> 8
    in
    Shared.fill_int32_be iobuf code;
    match t with
    | Ok | KerberosV5 | CleartextPassword | SCMCredential | GSS | SSPI -> ()
    | MD5Password { salt } -> Iobuf.Fill.stringo iobuf salt
    | GSSContinue { data } -> Iobuf.Fill.stringo iobuf data
  ;;
end

module ParameterDescription = struct
  type t = int array

  let message_type_char = Some 't'

  let consume_exn iobuf =
    let num_parameters = Iobuf.Consume.uint16_be iobuf in
    Array.init_ascending num_parameters ~f:(fun () -> Iobuf.Consume.int32_be iobuf)
  ;;

  let consume iobuf =
    match consume_exn iobuf with
    | exception exn ->
      error_s [%message "Failed to parse ParameterDescription" (exn : Exn.t)]
    | t -> Ok t
  ;;

  let validate_exn (_ : t) = ()

  let payload_length t =
    (* Number of parameters *) 2 (* Size of each parameter *) + (4 * Array.length t)
  ;;

  let fill t iobuf =
    Shared.fill_uint16_be iobuf (Array.length t);
    Array.iter t ~f:(fun i -> Shared.fill_int32_be iobuf i)
  ;;
end

module ParameterStatus = struct
  type t =
    { key : string
    ; data : string
    }
  [@@deriving sexp_of]

  let consume_exn iobuf =
    let key = Shared.consume_cstring_exn iobuf in
    let data = Shared.consume_cstring_exn iobuf in
    { key; data }
  ;;

  let consume iobuf =
    match consume_exn iobuf with
    | exception exn -> error_s [%message "Failed to parse ParameterStatus" (exn : Exn.t)]
    | t -> Ok t
  ;;

  let message_type_char = Some 'S'
  let validate_exn (_ : t) = ()
  let payload_length { key; data } = String.length key + 1 + String.length data + 1

  let fill { key; data } iobuf =
    Shared.fill_null_terminated iobuf key;
    Shared.fill_null_terminated iobuf data
  ;;
end

module BackendKeyData = struct
  type t = Types.backend_key

  let consume_exn iobuf =
    let pid = Iobuf.Consume.int32_be iobuf in
    let secret = Iobuf.Consume.int32_be iobuf in
    { Types.pid; secret }
  ;;

  let consume iobuf =
    match consume_exn iobuf with
    | exception exn -> error_s [%message "Failed to parse BackendKeyData" (exn : Exn.t)]
    | t -> Ok t
  ;;

  let message_type_char = Some 'K'
  let validate_exn (_ : t) = ()
  let payload_length _t = 8

  let fill ({ pid; secret } : Types.backend_key) iobuf =
    Shared.fill_int32_be iobuf pid;
    Shared.fill_int32_be iobuf secret
  ;;
end

module NotificationResponse = struct
  type t =
    { pid : Pid.t
    ; channel : Types.Notification_channel.t
    ; payload : string
    }
  [@@deriving sexp_of]

  let message_type_char = Some 'A'
  let validate_exn (_ : t) = ()

  let consume_exn iobuf =
    let pid = Pid.of_int (Iobuf.Consume.int32_be iobuf) in
    let channel =
      Shared.consume_cstring_exn iobuf |> Types.Notification_channel.of_string
    in
    let payload = Shared.consume_cstring_exn iobuf in
    { pid; channel; payload }
  ;;

  let consume iobuf =
    match consume_exn iobuf with
    | exception exn ->
      error_s [%message "Failed to parse NotificationResponse" (exn : Exn.t)]
    | t -> Ok t
  ;;

  let payload_length { channel; payload; _ } =
    (* PID length *)
    4
    (* Channel name and null terminator *)
    + (Types.Notification_channel.to_string channel |> String.length)
    + 1
    (* Payload string and null terminator *)
    + String.length payload
    + 1
  ;;

  let fill t iobuf =
    Shared.fill_int32_be iobuf (Pid.to_int t.pid);
    Shared.fill_null_terminated iobuf (Types.Notification_channel.to_string t.channel);
    Shared.fill_null_terminated iobuf t.payload
  ;;
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
    | other ->
      raise_s
        [%message "unrecognised backend status char in ReadyForQuery" (other : char)]
  ;;

  let consume iobuf =
    match consume_exn iobuf with
    | exception exn -> error_s [%message "Failed to parse ReadyForQuery" (exn : Exn.t)]
    | t -> Ok t
  ;;

  let message_type_char = Some 'Z'
  let validate_exn (_ : t) = ()
  let payload_length _t = 1

  let fill t iobuf =
    let code =
      match t with
      | Idle -> 'I'
      | In_transaction -> 'T'
      | In_failed_transaction -> 'E'
    in
    Iobuf.Fill.char iobuf code
  ;;
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

module CloseComplete = struct
  let consume (_ : _ Iobuf.t) = ()
end

module RowDescription = struct
  type t = Column_metadata.t iarray

  let consume_exn iobuf =
    let num_fields = Iobuf.Consume.uint16_be iobuf in
    Iarray.init_ascending num_fields ~f:(fun () ->
      let name = Shared.consume_cstring_exn iobuf in
      let skip =
        (* table *)
        4
        (* column *)
        + 2
      in
      Iobuf.advance iobuf skip;
      let pg_type_oid = Iobuf.Consume.int32_be iobuf in
      let skip =
        (* type modifier *)
        4
        (* length-or-variable *)
        + 2
      in
      Iobuf.advance iobuf skip;
      let format =
        match Iobuf.Consume.int16_be iobuf with
        | 0 -> `Text
        | 1 -> failwith "RowDescription format=Binary?"
        | i -> failwithf "RowDescription: bad format %i" i ()
      in
      Column_metadata.create ~name ~format ~pg_type_oid)
  ;;

  let consume iobuf =
    match consume_exn iobuf with
    | exception exn -> error_s [%message "Failed to parse RowDescription" (exn : Exn.t)]
    | cols -> Ok cols
  ;;
end

module DataRow = struct
  type t = string option iarray

  let consume_exn iobuf : t =
    let num_fields = Iobuf.Consume.uint16_be iobuf in
    Iarray.init_ascending num_fields ~f:(fun () ->
      match Iobuf.Consume.int32_be iobuf with
      | -1 -> None
      | len -> Some (Iobuf.Consume.stringo iobuf ~len))
  ;;

  let consume iobuf =
    match consume_exn iobuf with
    | exception exn -> error_s [%message "Failed to parse DataRow" (exn : Exn.t)]
    | t -> Ok t
  ;;

  let skip iobuf = Iobuf.advance iobuf (Iobuf.length iobuf)
  let message_type_char = Some 'D'
  let validate_exn (_ : t) = ()

  let payload_length t =
    let element_length elem =
      (* Column value length *)
      4
      +
      match elem with
      | None -> 0
      | Some string -> String.length string
    in
    (* Row size *)
    2 + Iarray.sum (module Int) t ~f:element_length
  ;;

  let fill (t : t) iobuf =
    let num_parameters = Iarray.length t in
    Shared.fill_uint16_be iobuf num_parameters;
    for idx = 0 to num_parameters - 1 do
      match Iarray.get t idx with
      | None -> Shared.fill_int32_be iobuf (-1)
      | Some str ->
        Shared.fill_int32_be iobuf (String.length str);
        Iobuf.Fill.stringo iobuf str
    done
  ;;
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
  [@@deriving compare, sexp_of]

  val consume : ([> read ], seek, Iobuf.global) Iobuf.t -> t Or_error.t
end

module CopyResponse (A : sig
    val name : string
  end) : CopyResponse = struct
  type column =
    { name : string
    ; format : [ `Text | `Binary ]
    }

  type t =
    { overall_format : [ `Text | `Binary ]
    ; num_columns : int
    ; column_formats : [ `Text | `Binary ] array
    }
  [@@deriving compare, sexp_of]

  let consume_exn iobuf =
    let overall_format =
      match Iobuf.Consume.int8 iobuf with
      | 0 -> `Text
      | 1 -> `Binary
      | i -> failwithf "%s: bad overall format: %i" A.name i ()
    in
    let num_columns = Iobuf.Consume.uint16_be iobuf in
    let column_formats =
      Array.init_ascending num_columns ~f:(fun () ->
        match Iobuf.Consume.int16_be iobuf with
        | 0 -> `Text
        | 1 -> `Binary
        | i -> failwithf "%s: bad format %i" A.name i ())
    in
    { overall_format; num_columns; column_formats }
  ;;

  let consume iobuf =
    match consume_exn iobuf with
    | cols -> Ok cols
    | exception exn ->
      let s = sprintf "Failed to parse %s" A.name in
      error_s [%message s (exn : Exn.t)]
  ;;
end

module CopyInResponse = CopyResponse (struct
    let name = "CopyInResponse"
  end)

module CopyOutResponse = CopyResponse (struct
    let name = "CopyOutResponse"
  end)

module CopyBothResponse = CopyResponse (struct
    let name = "CopyBothResponse"
  end)

module CommandComplete = struct
  type t = string

  let consume_exn = Shared.consume_cstring_exn

  let consume iobuf =
    match consume_exn iobuf with
    | exception exn -> error_s [%message "Failed to parse CommandComplete" (exn : Exn.t)]
    | t -> Ok t
  ;;

  let message_type_char = Some 'C'
  let validate_exn (_ : t) = ()
  let payload_length t = String.length t + 1
  let fill string iobuf = Shared.fill_null_terminated iobuf string
end

module No_arg : sig
  val bind_complete : string
  val close_complete : string
  val copy_done : string
  val empty_query_response : string
  val no_data : string
  val parse_complete : string
end = struct
  include Shared.No_arg

  let bind_complete = gen ~constructor:'2'
  let close_complete = gen ~constructor:'3'
  let copy_done = gen ~constructor:'c'
  let empty_query_response = gen ~constructor:'I'
  let no_data = gen ~constructor:'n'
  let parse_complete = gen ~constructor:'1'
end

module Writer = struct
  let write_message = Shared.write_message
  let auth_message = Staged.unstage (write_message (module AuthenticationRequest))
  let ready_for_query = Staged.unstage (write_message (module ReadyForQuery))
  let error_response = Staged.unstage (write_message (module ErrorResponse))
  let backend_key = Staged.unstage (write_message (module BackendKeyData))
  let parameter_description = Staged.unstage (write_message (module ParameterDescription))
  let parameter_status = Staged.unstage (write_message (module ParameterStatus))
  let command_complete = Staged.unstage (write_message (module CommandComplete))
  let data_row = Staged.unstage (write_message (module DataRow))
  let notice_response = Staged.unstage (write_message (module NoticeResponse))
  let copy_data = Staged.unstage (write_message (module Shared.CopyData))
  let notification_response = Staged.unstage (write_message (module NotificationResponse))
  let copy_done writer = Writer.write writer No_arg.copy_done
  let bind_complete writer = Writer.write writer No_arg.bind_complete
  let close_complete writer = Writer.write writer No_arg.close_complete
  let empty_query_response writer = Writer.write writer No_arg.empty_query_response
  let no_data writer = Writer.write writer No_arg.no_data
  let parse_complete writer = Writer.write writer No_arg.parse_complete
end
