open Core
open Int.Replace_polymorphic_compare

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

(* When [Word_size.word_size = W32], [int] can take at most 31 bits, so the max is 2**30.

   [Iobuf.Consume.int32_be] silently truncates. This is a shame (ideally it would return
   a [Int63.t]; see the comment at the top of [Int32.t], but otherwise an [Int32.t] would
   certainly suffice) and makes it quite annoying to safely implement something that reads
   32 bit ints on a 32 bit ocaml platform.

   Looking below, the 32 bit ints are

   - [message_length]: we'd refuse to read messages larger than 2**30 anyway,
   - [num_fields] (in a row): guaranteed to be < 1600 by postgres
     (https://www.postgresql.org/docs/11/ddl-basics.html),
   - [pid]: on linux, less than 2**22 (man 5 proc),
   - [secret] (from [BackendKeyData]): no guarantees.

   so, for now it seems safe enough to to stumble on in 32-bit-mode even though iobuf
   would silently truncate the ints. This is unsatisfying (besides not supporting reading
   [secret]) because if we've made a mistake, or have a bug, we'd rather crash on the
   protocol error than truncate.

   We'll revisit it if someone wants it. *)

module Shared = struct
  let validate_null_terminated_exn ~field_name str =
    if String.mem str '\x00'
    then raise_s [%message "String may not contain nulls" field_name str]
  ;;

  let fill_null_terminated iobuf str =
    Iobuf.Fill.stringo iobuf str;
    Iobuf.Fill.char iobuf '\x00'
  ;;

  let int16_min = -32768
  let int16_max = 32767

  let int32_min =
    match Word_size.word_size with
    | W64 -> Int32.to_int_exn Int32.min_value
    | W32 -> Int.min_value
  ;;

  let int32_max =
    match Word_size.word_size with
    | W64 -> Int32.to_int_exn Int32.max_value
    | W32 -> Int.max_value
  ;;

  let () =
    match Word_size.word_size with
    | W64 ->
      assert (String.equal (Int.to_string int32_min) "-2147483648");
      assert (String.equal (Int.to_string int32_max) "2147483647")
    | W32 ->
      assert (String.equal (Int.to_string int32_min) "-1073741824");
      assert (String.equal (Int.to_string int32_max) "1073741823")
  ;;

  let[@inline always] fill_int16_be iobuf value =
    match int16_min <= value && value <= int16_max with
    | true -> Iobuf.Fill.int16_be_trunc iobuf value
    | false -> failwithf "int16 out of range: %i" value ()
  ;;

  let[@inline always] fill_int32_be iobuf value =
    match int32_min <= value && value <= int32_max with
    | true -> Iobuf.Fill.int32_be_trunc iobuf value
    | false -> failwithf "int32 out of range: %i" value ()
  ;;

  let find_null_exn iobuf =
    let rec loop ~iobuf ~length ~pos =
      if Char.( = ) (Iobuf.Peek.char iobuf ~pos) '\x00'
      then pos
      else if pos > length - 1
      then failwith "find_null_exn could not find \\x00"
      else loop ~iobuf ~length ~pos:(pos + 1)
    in
    loop ~iobuf ~length:(Iobuf.length iobuf) ~pos:0
  ;;

  let consume_cstring_exn iobuf =
    let len = find_null_exn iobuf in
    let res = Iobuf.Consume.string iobuf ~len ~str_pos:0 in
    let zero = Iobuf.Consume.char iobuf in
    assert (Char.( = ) zero '\x00');
    res
  ;;

  module type Message_type = sig
    val message_type_char : char option

    type t

    val validate_exn : t -> unit
    val payload_length : t -> int
    val fill : t -> (read_write, Iobuf.seek) Iobuf.t -> unit
  end

  type 'a with_computed_length =
    { payload_length : int
    ; value : 'a
    }

  let write_message (type a) (module M : Message_type with type t = a) =
    let full_length { payload_length; _ } =
      match M.message_type_char with
      | None -> payload_length + 4
      | Some _ -> payload_length + 5
    in
    let blit_to_bigstring with_computed_length bigstring ~pos =
      let iobuf =
        Iobuf.of_bigstring bigstring ~pos ~len:(full_length with_computed_length)
      in
      (match M.message_type_char with
       | None -> ()
       | Some c -> Iobuf.Fill.char iobuf c);
      let { payload_length; value } = with_computed_length in
      fill_int32_be iobuf (payload_length + 4);
      M.fill value iobuf;
      match Iobuf.is_empty iobuf with
      | true -> ()
      | false -> failwith "postgres message filler lied about length"
    in
    Staged.stage (fun writer value ->
      M.validate_exn value;
      let payload_length = M.payload_length value in
      Async.Writer.write_gen_whole
        writer
        { payload_length; value }
        ~length:full_length
        ~blit_to_bigstring)
  ;;
end

module Frontend = struct
  module SSLRequest = struct
    let message_type_char = None

    type t = unit

    let payload_length () = 4
    let validate_exn () = ()

    (* These values look like dummy ones, but are the ones given in the postgres
       spec; the goal is to never collide with any protocol versions. *)
    let fill () iobuf =
      Iobuf.Fill.int16_be_trunc iobuf 1234;
      Iobuf.Fill.int16_be_trunc iobuf 5679
    ;;
  end

  module StartupMessage = struct
    let message_type_char = None

    type t =
      { user : string
      ; database : string
      ; replication : string option
      ; options : string list
      ; runtime_parameters : string String.Map.t
      }

    let validate_exn { user; database; replication; options; runtime_parameters } =
      Shared.validate_null_terminated_exn ~field_name:"user" user;
      Shared.validate_null_terminated_exn ~field_name:"database" database;
      (match replication with
       | Some replication ->
         Shared.validate_null_terminated_exn ~field_name:"replication" replication
       | None -> ());
      List.iter options ~f:(Shared.validate_null_terminated_exn ~field_name:"options");
      Map.iteri runtime_parameters ~f:(fun ~key:field_name ~data ->
        Shared.validate_null_terminated_exn ~field_name data;
        Shared.validate_null_terminated_exn ~field_name:(field_name ^ "-data") data)
    ;;

    let escape_option_space option =
      String.to_list option
      |> List.concat_map ~f:(fun char ->
        match char with
        | ' ' | '\\' -> [ '\\'; char ]
        | (_ : Char.t) -> [ char ])
      |> String.of_char_list
    ;;

    let payload_length { user; database; replication; options; runtime_parameters } =
      (* major and minor version: *)
      2
      + 2
      (* (key, value) pair for user (each null terminated): *)
      + 4
      + 1
      + String.length user
      + 1
      (* (key, value) pair for database (each null terminated): *)
      + 8
      + 1
      + String.length database
      + 1
      + (match replication with
        | Some replication -> 11 + 1 + String.length replication + 1
        | None -> 0)
      + (match List.is_empty options with
        | true -> 0
        | false ->
          7
          + 1
          + List.sum
              (module Int)
              options
              ~f:(fun value ->
                let escaped_value = escape_option_space value in
                1 + String.length escaped_value))
      (* end of list: *)
      + List.sum
          (module Int)
          (Map.to_alist runtime_parameters)
          ~f:(fun (key, data) -> String.length key + 1 + String.length data + 1)
      + 1
    ;;

    let fill { user; database; replication; options; runtime_parameters } iobuf =
      (* major *)
      Iobuf.Fill.int16_be_trunc iobuf 3;
      (* minor *)
      Iobuf.Fill.int16_be_trunc iobuf 0;
      Iobuf.Fill.stringo iobuf "user\x00";
      Shared.fill_null_terminated iobuf user;
      Iobuf.Fill.stringo iobuf "database\x00";
      Shared.fill_null_terminated iobuf database;
      (match replication with
       | None -> ()
       | Some replication ->
         Iobuf.Fill.stringo iobuf "replication\x00";
         Shared.fill_null_terminated iobuf replication);
      (match List.is_empty options with
       | true -> ()
       | false ->
         Iobuf.Fill.stringo iobuf "options\x00";
         List.map options ~f:escape_option_space
         |> String.concat ~sep:" "
         |> Shared.fill_null_terminated iobuf);
      Map.iteri runtime_parameters ~f:(fun ~key ~data ->
        Shared.fill_null_terminated iobuf key;
        Shared.fill_null_terminated iobuf data);
      Iobuf.Fill.char iobuf '\x00'
    ;;

    let parse_options_str s =
      let (_ : bool), parts, remainder =
        String.fold s ~init:(false, [], []) ~f:(fun (escaped, parts, current) char ->
          match char, escaped with
          | '\\', _ -> true, parts, current
          | ' ', true -> false, parts, ' ' :: current
          | ' ', false -> false, (List.rev current |> String.of_char_list) :: parts, []
          | _, false -> false, parts, char :: current
          | _, true -> false, parts, char :: '\\' :: current)
      in
      (List.rev remainder |> String.of_char_list) :: parts |> List.rev
    ;;

    let consume_exn iobuf =
      let (_protocol : int) = Iobuf.Consume.int32_be iobuf in
      let rec loop ~iobuf ~fields_rev =
        match Iobuf.Peek.char iobuf ~pos:0 with
        | '\x00' ->
          let (_ : Char.t) = Iobuf.Consume.char iobuf in
          List.rev fields_rev
        | _other ->
          let name = Shared.consume_cstring_exn iobuf |> String.lowercase in
          let value = Shared.consume_cstring_exn iobuf in
          loop ~iobuf ~fields_rev:((name, value) :: fields_rev)
      in
      let fields = loop ~iobuf ~fields_rev:[] |> String.Map.of_alist_exn in
      let user = Map.find_exn fields "user" in
      let database = Map.find fields "database" |> Option.value ~default:user in
      let replication = Map.find fields "replication" in
      let options =
        Map.find fields "options" |> Option.value_map ~default:[] ~f:parse_options_str
      in
      let runtime_parameters =
        List.fold
          [ "user"; "database"; "replication"; "options" ]
          ~init:fields
          ~f:Map.remove
      in
      { user; database; replication; options; runtime_parameters }
    ;;

    let consume iobuf =
      match consume_exn iobuf with
      | exception exn -> error_s [%message "Failed to parse StartupMessage" (exn : Exn.t)]
      | t -> Ok t
    ;;
  end

  module PasswordMessage = struct
    let message_type_char = Some 'p'

    type t =
      | Cleartext_or_md5_hex of string
      | Gss_binary_blob of string

    let validate_exn = function
      | Cleartext_or_md5_hex password ->
        Shared.validate_null_terminated_exn ~field_name:"password" password
      | Gss_binary_blob _ -> ()
    ;;

    let payload_length = function
      | Cleartext_or_md5_hex password -> String.length password + 1
      | Gss_binary_blob blob -> String.length blob
    ;;

    let fill t iobuf =
      match t with
      | Cleartext_or_md5_hex password -> Shared.fill_null_terminated iobuf password
      | Gss_binary_blob blob -> Iobuf.Fill.stringo iobuf blob
    ;;

    let consume_krb_exn iobuf ~length =
      let blob = Iobuf.Consume.string iobuf ~len:length ~str_pos:0 in
      Gss_binary_blob blob
    ;;

    let consume_krb iobuf ~length =
      match consume_krb_exn iobuf ~length with
      | exception exn ->
        error_s [%message "Failed to parse expected GSS PasswordMessage" (exn : Exn.t)]
      | t -> Ok t
    ;;

    let consume_password iobuf =
      match Shared.consume_cstring_exn iobuf with
      | exception exn ->
        error_s [%message "Failed to parse expected PasswordMessage" (exn : Exn.t)]
      | str -> Ok (Cleartext_or_md5_hex str)
    ;;
  end

  module Parse = struct
    let message_type_char = Some 'P'

    type t =
      { destination : Types.Statement_name.t
      ; query : string
      }

    let validate_exn t = Shared.validate_null_terminated_exn t.query ~field_name:"query"

    let payload_length t =
      String.length (Types.Statement_name.to_string t.destination)
      + 1
      + String.length t.query
      + 1
      + 2
    ;;

    let fill t iobuf =
      Shared.fill_null_terminated iobuf (Types.Statement_name.to_string t.destination);
      Shared.fill_null_terminated iobuf t.query;
      (* zero parameter types: *)
      Iobuf.Fill.int16_be_trunc iobuf 0
    ;;
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
      let parameter_length = function
        | None -> 0
        | Some s -> String.length s
      in
      (* destination and terminator *)
      String.length (Types.Portal_name.to_string t.destination)
      + 1
      (* statement and terminator *)
      + String.length (Types.Statement_name.to_string t.statement)
      + 1
      (* # parameter format codes = 1: *)
      + 2
      (* single parameter format code: *)
      + 2
      (* # parameters: *)
      + 2
      (* parameter sizes: *)
      + (4 * Array.length t.parameters)
      (* parameters *)
      + Array.sum (module Int) t.parameters ~f:parameter_length
      (* # result format codes = 1: *)
      + 2
      (* single result format code: *)
      + 2
    ;;

    let fill t iobuf =
      Shared.fill_null_terminated iobuf (Types.Portal_name.to_string t.destination);
      Shared.fill_null_terminated iobuf (Types.Statement_name.to_string t.statement);
      (* 1 parameter format code: *)
      Iobuf.Fill.int16_be_trunc iobuf 1;
      (* all parameters are text: *)
      Iobuf.Fill.int16_be_trunc iobuf 0;
      let num_parameters = Array.length t.parameters in
      Shared.fill_int16_be iobuf num_parameters;
      for idx = 0 to num_parameters - 1 do
        match t.parameters.(idx) with
        | None -> Shared.fill_int32_be iobuf (-1)
        | Some str ->
          Shared.fill_int32_be iobuf (String.length str);
          Iobuf.Fill.stringo iobuf str
      done;
      (* 1 result format code: *)
      Iobuf.Fill.int16_be_trunc iobuf 1;
      (* all results are text: *)
      Iobuf.Fill.int16_be_trunc iobuf 0
    ;;
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
        if n <= 0 then failwith "When provided, num rows limit must be positive"
    ;;

    let payload_length t = +String.length (Types.Portal_name.to_string t.portal) + 1 + 4

    let fill t iobuf =
      Shared.fill_null_terminated iobuf (Types.Portal_name.to_string t.portal);
      let limit =
        match t.limit with
        | Unlimited -> 0
        | Limit n -> n
      in
      Shared.fill_int32_be iobuf limit
    ;;
  end

  module Statement_or_portal_action = struct
    type t =
      | Statement of Types.Statement_name.t
      | Portal of Types.Portal_name.t

    let validate_exn (_t : t) = ()

    let payload_length t =
      let str =
        match t with
        | Statement s -> Types.Statement_name.to_string s
        | Portal s -> Types.Portal_name.to_string s
      in
      1 + String.length str + 1
    ;;

    let fill t iobuf =
      match t with
      | Statement s ->
        Iobuf.Fill.char iobuf 'S';
        Shared.fill_null_terminated iobuf (Types.Statement_name.to_string s)
      | Portal p ->
        Iobuf.Fill.char iobuf 'P';
        Shared.fill_null_terminated iobuf (Types.Portal_name.to_string p)
    ;;
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

    type t = { reason : string }

    let validate_exn t = Shared.validate_null_terminated_exn t.reason ~field_name:"reason"
    let payload_length t = String.length t.reason + 1
    let fill t iobuf = Shared.fill_null_terminated iobuf t.reason
  end

  module CopyData = struct
    let message_type_char = Some 'd'

    type t = string

    let validate_exn (_ : t) = ()
    let payload_length t = String.length t
    let fill t iobuf = Iobuf.Fill.stringo iobuf t
  end

  module CancelRequest = struct
    let message_type_char = None

    type t =
      { pid : int
      ; secret : int
      }

    let validate_exn (_ : t) = ()

    let payload_length (_ : t) =
      (* Cancel request code = 12345678 *)
      4
      + (* Pid*)
      4
      + (* Secret *)
      4
    ;;

    let fill t iobuf =
      Iobuf.Fill.int32_be_trunc iobuf 80877102;
      Iobuf.Fill.int32_be_trunc iobuf t.pid;
      Iobuf.Fill.int32_be_trunc iobuf t.secret
    ;;

    let consume_exn iobuf =
      let pid = Iobuf.Consume.int32_be iobuf in
      let secret = Iobuf.Consume.int32_be iobuf in
      { pid; secret }
    ;;

    let consume iobuf =
      match consume_exn iobuf with
      | exception exn -> error_s [%message "Failed to parse CacnelRequest" (exn : Exn.t)]
      | t -> Ok t
    ;;
  end

  module No_arg : sig
    val flush : string
    val sync : string
    val copy_done : string
    val terminate : string
  end = struct
    let gen ~constructor =
      let tmp = Iobuf.create ~len:5 in
      Iobuf.Poke.char tmp ~pos:0 constructor;
      (* fine to use Iobuf's int32 function, as [4] is clearly in range. *)
      Iobuf.Poke.int32_be_trunc tmp ~pos:1 4;
      Iobuf.to_string tmp
    ;;

    let flush = gen ~constructor:'H'
    let sync = gen ~constructor:'S'
    let copy_done = gen ~constructor:'c'
    let terminate = gen ~constructor:'X'
  end

  include No_arg

  module Writer = struct
    open Async

    let write_message = Shared.write_message
    let ssl_request = Staged.unstage (write_message (module SSLRequest))
    let startup_message = Staged.unstage (write_message (module StartupMessage))
    let password_message = Staged.unstage (write_message (module PasswordMessage))
    let parse = Staged.unstage (write_message (module Parse))
    let bind = Staged.unstage (write_message (module Bind))
    let close = Staged.unstage (write_message (module Close))
    let describe = Staged.unstage (write_message (module Describe))
    let execute = Staged.unstage (write_message (module Execute))
    let copy_fail = Staged.unstage (write_message (module CopyFail))
    let copy_data = Staged.unstage (write_message (module CopyData))
    let cancel_request = Staged.unstage (write_message (module CancelRequest))
    let flush writer = Writer.write writer No_arg.flush
    let sync writer = Writer.write writer No_arg.sync
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
  end

  module ErrorResponse = struct
    include Error_or_Notice

    let consume iobuf =
      match Error_or_Notice.consume_exn iobuf with
      | exception exn -> error_s [%message "Failed to parse ErrorResponse" (exn : Exn.t)]
      | t -> Ok t
    ;;

    let message_type_char = Some 'E'
    let validate_exn _t = ()

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

    let consume iobuf =
      match Error_or_Notice.consume_exn iobuf with
      | exception exn -> error_s [%message "Failed to parse NoticeResponse" (exn : Exn.t)]
      | t -> Ok t
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
      | other ->
        raise_s [%message "AuthenticationRequest unrecognised type" (other : int)]
    ;;

    let consume iobuf =
      match consume_exn iobuf with
      | exception exn ->
        error_s [%message "Failed to parse AuthenticationRequest" (exn : Exn.t)]
      | t -> Result.Ok t
    ;;

    let message_type_char = Some 'R'
    let validate_exn _t = ()

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

  module ParameterStatus = struct
    type t =
      { key : string
      ; data : string
      }

    let consume_exn iobuf =
      let key = Shared.consume_cstring_exn iobuf in
      let data = Shared.consume_cstring_exn iobuf in
      { key; data }
    ;;

    let consume iobuf =
      match consume_exn iobuf with
      | exception exn ->
        error_s [%message "Failed to parse ParameterStatus" (exn : Exn.t)]
      | t -> Ok t
    ;;

    let message_type_char = Some 'S'
    let validate_exn _t = ()
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
    let validate_exn _t = ()
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
    let validate_exn _t = ()
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

  module CopyDone = struct
    let consume (_ : _ Iobuf.t) = ()
  end

  module CloseComplete = struct
    let consume (_ : _ Iobuf.t) = ()
  end

  module RowDescription = struct
    type t = Column_metadata.t array

    let consume_exn iobuf =
      let num_fields = Iobuf.Consume.int16_be iobuf in
      Array.init_ascending num_fields ~f:(fun () ->
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
    type t = string option array

    let consume_exn iobuf =
      let num_fields = Iobuf.Consume.int16_be iobuf in
      Array.init_ascending num_fields ~f:(fun () ->
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
      | exception exn ->
        error_s [%message "Failed to parse CommandComplete" (exn : Exn.t)]
      | t -> Ok t
    ;;
  end

  module Writer = struct
    open! Async

    let write_message = Shared.write_message
    let auth_message = Staged.unstage (write_message (module AuthenticationRequest))
    let ready_for_query = Staged.unstage (write_message (module ReadyForQuery))
    let error_response = Staged.unstage (write_message (module ErrorResponse))
    let backend_key = Staged.unstage (write_message (module BackendKeyData))
    let parameter_status = Staged.unstage (write_message (module ParameterStatus))
  end
end
