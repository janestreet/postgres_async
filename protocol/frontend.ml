open! Core
open! Async
open! Import

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

  module Parameter = struct
    module Name = struct
      include Shared.Null_terminated_string.Nonempty

      let validate_exn t = Validate.maybe_raise (Validate.name "parameter" (validate t))
      let user = "user"
      let database = "database"
      let replication = "replication"
      let options = "options"

      (** "In addition to the above, other parameters may be listed. Parameter names
          beginning with _pq_. are reserved for use as protocol extensions, while others
          are treated as run-time parameters to be set at backend start time." *)
      let reserved_protocol_extension_prefix = "_pq_."

      let is_protocol_extension =
        String.is_prefix ~prefix:reserved_protocol_extension_prefix
      ;;
    end

    module Value = Shared.Null_terminated_string

    module Options = struct
      let escape_char = '\\'
      let escapeworthy = lazy (List.filter [%all: Char.t] ~f:Char.is_whitespace)

      let encode =
        let escape =
          lazy
            (unstage
               (String.Escaping.escape ~escapeworthy:(force escapeworthy) ~escape_char))
        in
        fun options ->
          List.map options ~f:(fun s ->
            if String.is_empty s
            then invalid_arg "cannot encode empty arguments"
            else if String.mem s '\x00'
            then invalid_arg "cannot encode null characters"
            else force escape s)
          |> String.concat ~sep:" "
      ;;

      let decode =
        let unescape = lazy (unstage (String.Escaping.unescape ~escape_char)) in
        fun s ->
          String.Escaping.split_on_chars s ~on:(force escapeworthy) ~escape_char
          |> List.filter_map ~f:(function
            | "" -> None
            | s -> Some (force unescape s))
      ;;

      let%test_unit "round-trip" =
        let generator =
          String.gen_nonempty'
            Quickcheck.Generator.(
              union
                [ of_list (force escapeworthy)
                ; return escape_char
                ; Char.gen_uniform_inclusive '\x01' Char.max_value
                ])
        in
        Quickcheck.test (List.gen_non_empty generator) ~f:(fun original ->
          [%test_result: string list] (decode (encode original)) ~expect:original)
      ;;
    end
  end

  (** The protocol version number. The most significant 16 bits are the major version
      number (3 for the protocol described here). The least significant 16 bits are the
      minor version number (0 for the protocol described here). *)
  let this_protocol = 0x00030000

  (** postgres sets an arbitrary limit on startup packet length to prevent DoS. This limit
      has been unchanged from 2003-2024 so it seems pretty reasonable to hard code here. *)
  let max_startup_packet_length = 10_000

  type t = Parameter.Value.t Map.M(Parameter.Name).t
  [@@deriving compare, quickcheck, sexp_of]

  let quickcheck_generator =
    let open Quickcheck.Generator.Let_syntax in
    let%map user = [%quickcheck.generator: Parameter.Value.t]
    and t = [%quickcheck.generator: t] in
    Map.set t ~key:Parameter.Name.user ~data:user
  ;;

  let find = Map.find

  (** "user" is the only required parameter *)
  let user t =
    let key = Parameter.Name.user in
    match find t key with
    | Some user -> user
    | None -> raise_s [%sexp (key : Parameter.Name.t), "is missing from startup message"]
  ;;

  let database_defaulting_to_user t =
    match find t Parameter.Name.database with
    | Some database -> database
    | None -> user t
  ;;

  let options t =
    match Map.find t Parameter.Name.options with
    | None -> []
    | Some options -> Parameter.Options.decode options
  ;;

  let runtime_parameters t =
    List.fold
      Parameter.Name.[ user; database; options; replication ]
      ~init:t
      ~f:Map.remove
    |> Map.filter_keys ~f:(Fn.non Parameter.Name.is_protocol_extension)
  ;;

  let protocol_extensions t = Map.filter_keys t ~f:Parameter.Name.is_protocol_extension

  let payload_length t =
    (* protocol version number: *)
    4
    (* parameters: *)
    + Map.sumi (module Int) t ~f:(fun ~key ~data ->
      Parameter.Name.payload_length key + Parameter.Value.payload_length data)
    (* trailing null byte *)
    + 1
  ;;

  let validate_exn t =
    let (_ : string) =
      (* ensure the required parameter is present *)
      user t
    in
    Map.iteri t ~f:(fun ~key:field_name ~data ->
      Parameter.Name.validate_exn field_name;
      Validate.maybe_raise (Validate.name field_name (Parameter.Value.validate data)));
    if payload_length t > max_startup_packet_length
    then (
      let largest_field =
        Map.to_alist t
        |> List.max_elt
             ~compare:
               (Comparable.lift [%compare: int] ~f:(fun (key, data) ->
                  String.length key + String.length data))
        |> Option.value_exn
        |> fst
      in
      raise_s [%sexp "StartupMessage is too large", ~~(largest_field : string)])
  ;;

  let of_parameters_exn t =
    validate_exn t;
    t
  ;;

  let create_exn
    ~user
    ?database
    ?replication
    ?options
    ?runtime_parameters:(unvalidated_runtime_parameters = String.Map.empty)
    ?(protocol_extensions = String.Map.empty)
    ()
    =
    let runtime_parameters = runtime_parameters unvalidated_runtime_parameters in
    let () =
      let invalid_runtime_parameters =
        Set.diff
          (Map.key_set unvalidated_runtime_parameters)
          (Map.key_set runtime_parameters)
      in
      if not (Set.is_empty invalid_runtime_parameters)
      then raise_s [%sexp ~~(invalid_runtime_parameters : String.Set.t)]
    in
    let () =
      let invalid_protocol_extensions =
        Map.key_set protocol_extensions
        |> Set.filter ~f:(Fn.non Parameter.Name.is_protocol_extension)
      in
      if not (Set.is_empty invalid_protocol_extensions)
      then raise_s [%sexp ~~(invalid_protocol_extensions : String.Set.t)]
    in
    Map.merge_disjoint_exn runtime_parameters protocol_extensions
    |> Map.add_exn ~key:Parameter.Name.user ~data:user
    |> (match database with
      | None -> Fn.id
      | Some data -> Map.add_exn ~key:Parameter.Name.database ~data)
    |> (match options with
      | None -> Fn.id
      | Some options ->
        Map.add_exn ~key:Parameter.Name.options ~data:(Parameter.Options.encode options))
    |> (match replication with
      | None -> Fn.id
      | Some data -> Map.add_exn ~key:Parameter.Name.replication ~data)
    |> of_parameters_exn
  ;;

  let fill t iobuf =
    validate_exn t;
    Iobuf.Fill.int32_be_trunc iobuf this_protocol;
    Map.iteri t ~f:(fun ~key ~data ->
      Parameter.Name.fill iobuf key;
      Parameter.Value.fill iobuf data);
    Iobuf.Fill.char iobuf '\x00'
  ;;

  let consume_exn (local_ iobuf) =
    let rec consume_parameters ~(local_ iobuf) ~parameters =
      match Parameter.Name.consume_exn iobuf with
      | Error `Empty_string -> of_parameters_exn parameters
      | Ok key ->
        let data = Parameter.Value.consume_exn iobuf in
        consume_parameters ~iobuf ~parameters:(Map.set parameters ~key ~data)
    in
    if Iobuf.length iobuf > max_startup_packet_length
    then failwith "StartupMessage is too large";
    let protocol = Iobuf.Consume.int32_be iobuf in
    [%test_result: int] protocol ~expect:this_protocol;
    consume_parameters ~iobuf ~parameters:String.Map.empty [@nontail]
  ;;

  let%test_unit "round-trip" =
    Quickcheck.test [%quickcheck.generator: t] ~f:(fun original ->
      let len = payload_length original in
      let iobuf = Iobuf.create ~len in
      fill original iobuf;
      assert (Iobuf.is_empty iobuf);
      Iobuf.flip_lo iobuf;
      let consumed = consume_exn iobuf in
      assert (Iobuf.is_empty iobuf);
      [%test_result: t] consumed ~expect:original)
  ;;

  let consume (local_ iobuf) =
    match consume_exn iobuf with
    | t -> Ok t
    | exception exn ->
      Or_error.of_exn exn |> Or_error.tag ~tag:"Failed to parse StartupMessage"
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
    + (4 * Array.length t.parameters) (* parameters *)
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
    Shared.fill_uint16_be iobuf num_parameters;
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
    | Limit n -> if n <= 0 then failwith "When provided, num rows limit must be positive"
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

  let validate_exn (_ : t) = ()

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

module Query = struct
  type t = string

  let message_type_char = Some 'Q'
  let consume_exn = Shared.consume_cstring_exn

  let consume iobuf =
    match consume_exn iobuf with
    | exception exn -> error_s [%message "Failed to parse Query" (exn : Exn.t)]
    | t -> Ok t
  ;;

  let validate_exn (_ : t) = ()
  let payload_length t = 1 + String.length t
  let fill t iobuf = Shared.fill_null_terminated iobuf t
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
  include Shared.No_arg

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
  let query = Staged.unstage (write_message (module Query))
  let describe = Staged.unstage (write_message (module Describe))
  let execute = Staged.unstage (write_message (module Execute))
  let copy_fail = Staged.unstage (write_message (module CopyFail))
  let copy_data = Staged.unstage (write_message (module Shared.CopyData))
  let cancel_request = Staged.unstage (write_message (module CancelRequest))
  let flush writer = Writer.write writer No_arg.flush
  let sync writer = Writer.write writer No_arg.sync
  let copy_done writer = Writer.write writer No_arg.copy_done
  let terminate writer = Writer.write writer No_arg.terminate
end
