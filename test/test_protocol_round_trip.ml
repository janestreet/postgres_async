open Core
open Async
module Protocol = Postgres_async.Private.Protocol

let roundtrip ~write ~read =
  let reader, pipe_writer = Pipe.create () in
  let%bind writer, _flushed = Writer.of_pipe (Info.create_s [%message "w"]) pipe_writer in
  write writer;
  let closed = Writer.close writer in
  let%bind contents =
    Reader.of_pipe (Info.create_s [%message "r"]) reader >>= Reader.contents
  in
  let iobuf = Iobuf.of_string contents in
  read iobuf;
  closed
;;

let read_message iobuf ~read_message_type_char ~read_payload =
  let message_char =
    match read_message_type_char with
    | true -> Iobuf.Consume.char iobuf |> Option.some
    | false -> None
  in
  print_s [%message (message_char : Char.t option)];
  let length = Iobuf.Consume.int32_be iobuf in
  print_s [%message (length : int)];
  (* Length includes itself*)
  let payload_length = length - 4 in
  read_payload ~payload_length iobuf
;;

let test_startup ?replication ?(options = []) ?(runtime_parameters = String.Map.empty) () =
  let write writer =
    Protocol.Frontend.Writer.startup_message
      writer
      { user = "test-user"
      ; database = "testdb"
      ; options
      ; runtime_parameters
      ; replication
      }
  in
  let read_payload ~payload_length:_ iobuf =
    let ({ user; database; options; runtime_parameters; replication }
         : Protocol.Frontend.StartupMessage.t)
      =
      Protocol.Frontend.StartupMessage.consume iobuf |> Or_error.ok_exn
    in
    print_s
      [%message
        (user : string)
          (database : string)
          (replication : string option)
          (options : string list)
          (runtime_parameters : string String.Map.t)]
  in
  let read = read_message ~read_message_type_char:false ~read_payload in
  roundtrip ~write ~read
;;

let%expect_test "Simple Startup" =
  let%bind () = test_startup () in
  [%expect
    {|
    (message_char ())
    (length 40)
    ((user test-user) (database testdb) (replication ()) (options ())
     (runtime_parameters ())) |}];
  Deferred.unit
;;

let%expect_test "Replication Startup" =
  let%bind () = test_startup ~replication:"database" () in
  [%expect
    {|
    (message_char ())
    (length 61)
    ((user test-user) (database testdb) (replication (database)) (options ())
     (runtime_parameters ())) |}];
  Deferred.unit
;;

let%expect_test "Startup with Params" =
  let%bind () =
    test_startup
      ~runtime_parameters:
        (String.Map.of_alist_exn [ "client_encoding", "UTF8"; "DateStyle", "ISO,MDY" ])
      ()
  in
  [%expect
    {|
    (message_char ())
    (length 79)
    ((user test-user) (database testdb) (replication ()) (options ())
     (runtime_parameters ((client_encoding UTF8) (datestyle ISO,MDY)))) |}];
  Deferred.unit
;;

(* Note : options is deprecated in favor of runtime parameters according
   to postgres docs *)
let%expect_test "Startup with Options" =
  let%bind () =
    test_startup ~options:[ "client_encoding=UTF8"; "statement_timeout=3000" ] ()
  in
  [%expect
    {|
    (message_char ())
    (length 92)
    ((user test-user) (database testdb) (replication ())
     (options (client_encoding=UTF8 statement_timeout=3000))
     (runtime_parameters ())) |}];
  Deferred.unit
;;

let%expect_test "Startup with Options with space" =
  let%bind () =
    test_startup
      ~options:[ "application_name='More Spaces Please'"; "statement_timeout=3000" ]
      ()
  in
  [%expect
    {|
    (message_char ())
    (length 111)
    ((user test-user) (database testdb) (replication ())
     (options ("application_name='More Spaces Please'" statement_timeout=3000))
     (runtime_parameters ())) |}];
  Deferred.unit
;;

let%expect_test "Startup with Options with multi space and backslash" =
  let%bind () =
    test_startup
      ~options:
        [ "--application_name='My \\very complicated   a\\p\\p name'"
        ; "--statement_timeout=3000"
        ]
      ()
  in
  [%expect
    {|
    (message_char ())
    (length 137)
    ((user test-user) (database testdb) (replication ())
     (options
      ("--application_name='My \\very complicated   a\\p\\p name'"
       --statement_timeout=3000))
     (runtime_parameters ())) |}];
  Deferred.unit
;;

let%expect_test "KRB password message " =
  let write writer =
    Protocol.Frontend.Writer.password_message writer (Gss_binary_blob "blob")
  in
  let read_payload ~payload_length iobuf =
    let msg =
      Protocol.Frontend.PasswordMessage.consume_krb iobuf ~length:payload_length
      |> Or_error.ok_exn
    in
    match (msg : Protocol.Frontend.PasswordMessage.t) with
    | Gss_binary_blob blob -> print_s [%message (blob : string)]
    | Cleartext_or_md5_hex _ -> raise_s [%message "Expected krb"]
  in
  let read = read_message ~read_message_type_char:true ~read_payload in
  let%bind () = roundtrip ~write ~read in
  [%expect {|
    (message_char (p))
    (length 8)
    (blob blob) |}];
  Deferred.unit
;;

let%expect_test "Password password message " =
  let write writer =
    Protocol.Frontend.Writer.password_message writer (Cleartext_or_md5_hex "hex")
  in
  let read_payload ~payload_length:_ iobuf =
    let msg =
      Protocol.Frontend.PasswordMessage.consume_password iobuf |> Or_error.ok_exn
    in
    match (msg : Protocol.Frontend.PasswordMessage.t) with
    | Gss_binary_blob _ -> raise_s [%message "Got krb"]
    | Cleartext_or_md5_hex blob -> print_s [%message (blob : string)]
  in
  let read = read_message ~read_message_type_char:true ~read_payload in
  let%bind () = roundtrip ~write ~read in
  [%expect {|
    (message_char (p))
    (length 8)
    (blob hex) |}];
  Deferred.unit
;;

let%expect_test "Authentication Request message " =
  let run sent =
    let write writer = Protocol.Backend.Writer.auth_message writer sent in
    let read_payload ~payload_length:_ iobuf =
      let msg = Protocol.Backend.AuthenticationRequest.consume iobuf |> Or_error.ok_exn in
      print_s
        [%message
          (sent : Protocol.Backend.AuthenticationRequest.t)
            (msg : Protocol.Backend.AuthenticationRequest.t)]
    in
    let read = read_message ~read_message_type_char:true ~read_payload in
    roundtrip ~write ~read
  in
  let%bind () =
    Deferred.List.iter
      ~how:`Sequential
      ~f:run
      Protocol.Backend.AuthenticationRequest.
        [ Ok
        ; KerberosV5
        ; CleartextPassword
        ; MD5Password { salt = "salt" }
        ; SCMCredential
        ; GSS
        ; SSPI
        ; GSSContinue { data = "gsscont" }
        ]
  in
  [%expect
    {|
    (message_char (R))
    (length 8)
    ((sent Ok) (msg Ok))
    (message_char (R))
    (length 8)
    ((sent KerberosV5) (msg KerberosV5))
    (message_char (R))
    (length 8)
    ((sent CleartextPassword) (msg CleartextPassword))
    (message_char (R))
    (length 12)
    ((sent (MD5Password (salt salt))) (msg (MD5Password (salt salt))))
    (message_char (R))
    (length 8)
    ((sent SCMCredential) (msg SCMCredential))
    (message_char (R))
    (length 8)
    ((sent GSS) (msg GSS))
    (message_char (R))
    (length 8)
    ((sent SSPI) (msg SSPI))
    (message_char (R))
    (length 15)
    ((sent (GSSContinue (data gsscont))) (msg (GSSContinue (data gsscont)))) |}];
  Deferred.unit
;;

let%expect_test "Authentication Request message " =
  let run sent =
    let write writer = Protocol.Backend.Writer.ready_for_query writer sent in
    let read_payload ~payload_length:_ iobuf =
      let msg = Protocol.Backend.ReadyForQuery.consume iobuf |> Or_error.ok_exn in
      print_s
        [%message
          (sent : Protocol.Backend.ReadyForQuery.t)
            (msg : Protocol.Backend.ReadyForQuery.t)]
    in
    let read = read_message ~read_message_type_char:true ~read_payload in
    roundtrip ~write ~read
  in
  let%bind () =
    Deferred.List.iter
      ~how:`Sequential
      ~f:run
      Protocol.Backend.ReadyForQuery.[ Idle; In_transaction; In_failed_transaction ]
  in
  [%expect
    {|
    (message_char (Z))
    (length 5)
    ((sent Idle) (msg Idle))
    (message_char (Z))
    (length 5)
    ((sent In_transaction) (msg In_transaction))
    (message_char (Z))
    (length 5)
    ((sent In_failed_transaction) (msg In_failed_transaction)) |}];
  Deferred.unit
;;

let%expect_test "BackendKeyData " =
  let write writer =
    Protocol.Backend.Writer.backend_key writer { pid = 1234; secret = 4444 }
  in
  let read_payload ~payload_length:_ iobuf =
    let ({ pid; secret } : Protocol.Backend.BackendKeyData.t) =
      Protocol.Backend.BackendKeyData.consume iobuf |> Or_error.ok_exn
    in
    print_s [%message (pid : int) (secret : int)]
  in
  let read = read_message ~read_message_type_char:true ~read_payload in
  let%bind () = roundtrip ~write ~read in
  [%expect {|
    (message_char (K))
    (length 12)
    ((pid 1234) (secret 4444)) |}];
  Deferred.unit
;;

let%expect_test "Paramter Status " =
  let write writer =
    Protocol.Backend.Writer.parameter_status
      writer
      { key = "server_version"; data = "13.6" }
  in
  let read_payload ~payload_length:_ iobuf =
    let ({ key; data } : Protocol.Backend.ParameterStatus.t) =
      Protocol.Backend.ParameterStatus.consume iobuf |> Or_error.ok_exn
    in
    print_s [%message (key : string) (data : string)]
  in
  let read = read_message ~read_message_type_char:true ~read_payload in
  let%bind () = roundtrip ~write ~read in
  [%expect
    {|
    (message_char (S))
    (length 24)
    ((key server_version) (data 13.6)) |}];
  Deferred.unit
;;

let%expect_test "Error Response " =
  let write writer =
    Protocol.Backend.Writer.error_response
      writer
      { error_code = "28000"; all_fields = [ Severity, "FATAL"; Message, "auth-error" ] }
  in
  let read_payload ~payload_length:_ iobuf =
    let ({ error_code; all_fields } : Protocol.Backend.ErrorResponse.t) =
      Protocol.Backend.ErrorResponse.consume iobuf |> Or_error.ok_exn
    in
    print_s
      [%message
        (error_code : string)
          (all_fields : (Protocol.Backend.Error_or_notice_field.t * string) list)]
  in
  let read = read_message ~read_message_type_char:true ~read_payload in
  let%bind () = roundtrip ~write ~read in
  [%expect
    {|
    (message_char (E))
    (length 31)
    ((error_code 28000)
     (all_fields ((Code 28000) (Severity FATAL) (Message auth-error)))) |}];
  Deferred.unit
;;
