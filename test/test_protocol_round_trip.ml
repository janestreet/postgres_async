open Core
open Async
module Protocol = Postgres_async.Private.Protocol

let roundtrip ~write ~read =
  let%bind `Reader reader_fd, `Writer writer_fd =
    Unix.pipe (Info.of_string "roundtrip")
  in
  let%map () =
    let writer = Writer.create writer_fd in
    Writer.with_close writer ~f:(fun () ->
      write writer;
      return ())
  and contents = Reader.contents (Reader.create reader_fd) in
  read (Iobuf.of_string contents)
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
  (* Length includes itself *)
  let payload_length = length - 4 in
  read_payload ~payload_length iobuf
;;

let test_startup ?replication ?options runtime_parameters =
  let startup_message =
    Protocol.Frontend.StartupMessage.create_exn
      ()
      ~user:"test-user"
      ~database:"testdb"
      ?options
      ?replication
      ~runtime_parameters:(String.Map.of_alist_exn runtime_parameters)
  in
  let write writer = Protocol.Frontend.Writer.startup_message writer startup_message in
  let read_payload ~payload_length:_ iobuf =
    let read = Protocol.Frontend.StartupMessage.consume iobuf |> ok_exn in
    [%test_result: Protocol.Frontend.StartupMessage.t] read ~expect:startup_message;
    print_s [%sexp (read : Protocol.Frontend.StartupMessage.t)]
  in
  let read = read_message ~read_message_type_char:false ~read_payload in
  roundtrip ~write ~read
;;

let%expect_test "Simple Startup" =
  let%bind () = test_startup [] in
  [%expect
    {|
    (message_char ())
    (length 40)
    ((database testdb) (user test-user))
    |}];
  Deferred.unit
;;

let%expect_test "Replication Startup" =
  let%bind () = test_startup ~replication:"database" [] in
  [%expect
    {|
    (message_char ())
    (length 61)
    ((database testdb) (replication database) (user test-user))
    |}];
  Deferred.unit
;;

let%expect_test "Startup with Params" =
  let%bind () = test_startup [ "client_encoding", "UTF8"; "DateStyle", "ISO,MDY" ] in
  [%expect
    {|
    (message_char ())
    (length 79)
    ((DateStyle ISO,MDY) (client_encoding UTF8) (database testdb)
     (user test-user))
    |}];
  Deferred.unit
;;

(* Note : options is deprecated in favor of runtime parameters according to postgres docs *)

let%expect_test "Startup with Options" =
  let%bind () =
    test_startup [] ~options:[ "client_encoding=UTF8"; "statement_timeout=3000" ]
  in
  [%expect
    {|
    (message_char ())
    (length 92)
    ((database testdb) (options "client_encoding=UTF8 statement_timeout=3000")
     (user test-user))
    |}];
  Deferred.unit
;;

let%expect_test "Startup with Options with space" =
  let%bind () =
    test_startup
      []
      ~options:[ "application_name='More Spaces Please'"; "statement_timeout=3000" ]
  in
  [%expect
    {|
    (message_char ())
    (length 111)
    ((database testdb)
     (options "application_name='More\\ Spaces\\ Please' statement_timeout=3000")
     (user test-user))
    |}];
  Deferred.unit
;;

let%expect_test "Startup with Options with multi space and backslash" =
  let%bind () =
    test_startup
      []
      ~options:
        [ "--application_name='My \\very complicated   a\\p\\p name'"
        ; "--statement_timeout=3000"
        ]
  in
  [%expect
    {|
    (message_char ())
    (length 137)
    ((database testdb)
     (options
      "--application_name='My\\ \\\\very\\ complicated\\ \\ \\ a\\\\p\\\\p\\ name' --statement_timeout=3000")
     (user test-user))
    |}];
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
    | Cleartext_or_md5_hex _ | Sasl_initial_response _ | Sasl_response _ ->
      raise_s [%message "Expected krb"]
  in
  let read = read_message ~read_message_type_char:true ~read_payload in
  let%bind () = roundtrip ~write ~read in
  [%expect
    {|
    (message_char (p))
    (length 8)
    (blob blob)
    |}];
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
    | Gss_binary_blob _ | Sasl_initial_response _ | Sasl_response _ ->
      raise_s [%message "Got non-password message"]
    | Cleartext_or_md5_hex blob -> print_s [%message (blob : string)]
  in
  let read = read_message ~read_message_type_char:true ~read_payload in
  let%bind () = roundtrip ~write ~read in
  [%expect
    {|
    (message_char (p))
    (length 8)
    (blob hex)
    |}];
  Deferred.unit
;;

let%expect_test "SASL initial response password message" =
  let write writer =
    Protocol.Frontend.Writer.password_message
      writer
      (Sasl_initial_response
         { mechanism = "SCRAM-SHA-256"; initial_response = "n,,n=,r=nonce" })
  in
  let read_payload ~payload_length:_ iobuf =
    let msg =
      Protocol.Frontend.PasswordMessage.consume_sasl_initial_response iobuf
      |> Or_error.ok_exn
    in
    match (msg : Protocol.Frontend.PasswordMessage.t) with
    | Sasl_initial_response { mechanism; initial_response } ->
      print_s [%message (mechanism : string) (initial_response : string)]
    | Cleartext_or_md5_hex _ | Gss_binary_blob _ | Sasl_response _ ->
      raise_s [%message "Got non-SASL initial response message"]
  in
  let read = read_message ~read_message_type_char:true ~read_payload in
  let%bind () = roundtrip ~write ~read in
  [%expect
    {|
    (message_char (p))
    (length 35)
    ((mechanism SCRAM-SHA-256) (initial_response n,,n=,r=nonce))
    |}];
  Deferred.unit
;;

let%expect_test "SASL response password message" =
  let write writer =
    Protocol.Frontend.Writer.password_message writer (Sasl_response "proof")
  in
  let read_payload ~payload_length:_ iobuf =
    let msg =
      Protocol.Frontend.PasswordMessage.consume_sasl_response iobuf |> Or_error.ok_exn
    in
    match (msg : Protocol.Frontend.PasswordMessage.t) with
    | Sasl_response response -> print_s [%message (response : string)]
    | Cleartext_or_md5_hex _ | Gss_binary_blob _ | Sasl_initial_response _ ->
      raise_s [%message "Got non-SASL response message"]
  in
  let read = read_message ~read_message_type_char:true ~read_payload in
  let%bind () = roundtrip ~write ~read in
  [%expect
    {|
    (message_char (p))
    (length 9)
    (response proof)
    |}];
  Deferred.unit
;;

let test_bind () =
  let write writer =
    Protocol.Frontend.Writer.bind
      writer
      { destination = Protocol.Portal_name.unnamed
      ; statement = Protocol.Statement_name.unnamed
      ; parameters = [| Some "x"; None |]
      }
  in
  roundtrip ~write ~read:(fun iobuf ->
    Iobuf.Consume.stringo iobuf |> String.Hexdump.to_string_hum |> print_endline)
;;

let test_bind_with_result_formats result_formats =
  let write writer =
    Protocol.Frontend.Writer.bind_with_result_formats
      writer
      { destination = Protocol.Portal_name.unnamed
      ; statement = Protocol.Statement_name.unnamed
      ; parameters = [| Some "x"; None |]
      ; result_formats
      }
  in
  roundtrip ~write ~read:(fun iobuf ->
    Iobuf.Consume.stringo iobuf |> String.Hexdump.to_string_hum |> print_endline)
;;

let%expect_test "Bind with the default text result format" =
  let%bind () = test_bind () in
  [%expect
    {|
    00000000  42 00 00 00 19 00 00 00  01 00 00 00 02 00 00 00  |B...............|
    00000010  01 78 ff ff ff ff 00 01  00 00                    |.x........|
    |}];
  Deferred.unit
;;

let%expect_test "Bind with mixed result formats" =
  let%bind () =
    test_bind_with_result_formats (Iarray.of_list [ Protocol.Format.Text; Binary ])
  in
  [%expect
    {|
    00000000  42 00 00 00 1b 00 00 00  01 00 00 00 02 00 00 00  |B...............|
    00000010  01 78 ff ff ff ff 00 02  00 00 00 01              |.x..........|
    |}];
  Deferred.unit
;;

let%expect_test "Bind compresses uniform binary result formats" =
  let%bind () =
    test_bind_with_result_formats (Iarray.of_list [ Protocol.Format.Binary; Binary ])
  in
  [%expect
    {|
    00000000  42 00 00 00 19 00 00 00  01 00 00 00 02 00 00 00  |B...............|
    00000010  01 78 ff ff ff ff 00 01  00 01                    |.x........|
    |}];
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
        ; SASL
            { mechanisms = Iarray.of_array [| "SCRAM-SHA-256"; "SCRAM-SHA-256-PLUS" |] }
        ; SASLContinue { data = "server-first" }
        ; SASLFinal { data = "server-final" }
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
    ((sent (GSSContinue (data gsscont))) (msg (GSSContinue (data gsscont))))
    (message_char (R))
    (length 42)
    ((sent (SASL (mechanisms (SCRAM-SHA-256 SCRAM-SHA-256-PLUS))))
     (msg (SASL (mechanisms (SCRAM-SHA-256 SCRAM-SHA-256-PLUS)))))
    (message_char (R))
    (length 20)
    ((sent (SASLContinue (data server-first)))
     (msg (SASLContinue (data server-first))))
    (message_char (R))
    (length 20)
    ((sent (SASLFinal (data server-final)))
     (msg (SASLFinal (data server-final))))
    |}];
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
    ((sent In_failed_transaction) (msg In_failed_transaction))
    |}];
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
  [%expect
    {|
    (message_char (K))
    (length 12)
    ((pid 1234) (secret 4444))
    |}];
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
    ((key server_version) (data 13.6))
    |}];
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
     (all_fields ((Code 28000) (Severity FATAL) (Message auth-error))))
    |}];
  Deferred.unit
;;

let row_description_iobuf fields =
  let iobuf = Iobuf.create ~len:256 in
  Iobuf.Fill.int16_be_trunc iobuf (List.length fields);
  List.iter fields ~f:(fun (name, pg_type_oid, format) ->
    Iobuf.Fill.stringo iobuf name;
    Iobuf.Fill.char iobuf '\000';
    Iobuf.Fill.int32_be_trunc iobuf 0;
    Iobuf.Fill.int16_be_trunc iobuf 0;
    Iobuf.Fill.int32_be_trunc iobuf pg_type_oid;
    Iobuf.Fill.int16_be_trunc iobuf (-1);
    Iobuf.Fill.int32_be_trunc iobuf (-1);
    Iobuf.Fill.int16_be_trunc iobuf format);
  Iobuf.flip_lo iobuf;
  iobuf
;;

let%expect_test "Row Description with mixed formats" =
  let iobuf = row_description_iobuf [ "text", 25, 0; "bytes", 17, 1 ] in
  let columns = Protocol.Backend.RowDescription.consume iobuf |> Or_error.ok_exn in
  print_s [%sexp (columns : Protocol.Column_metadata.t iarray)];
  Iarray.iter columns ~f:(fun column ->
    let name = Protocol.Column_metadata.name column in
    let pg_type_oid = Protocol.Column_metadata.pg_type_oid column in
    let format = Protocol.Column_metadata.format column in
    print_s [%message (name : string) (pg_type_oid : int) (format : Protocol.Format.t)]);
  [%expect
    {|
    (((name text) (format Text) (pg_type_oid 25))
     ((name bytes) (format Binary) (pg_type_oid 17)))
    ((name text) (pg_type_oid 25) (format Text))
    ((name bytes) (pg_type_oid 17) (format Binary))
    |}];
  Deferred.unit
;;

let%expect_test "Data Row" =
  let write writer =
    Protocol.Backend.Writer.data_row
      writer
      (Iarray.of_list [ Some "Col 1"; Some "Col 2"; None; Some "Col 4" ])
  in
  let read_payload ~payload_length:_ iobuf =
    let (row : string option iarray) =
      Protocol.Backend.DataRow.consume iobuf |> Or_error.ok_exn
    in
    print_s [%message (row : string option iarray)]
  in
  let read = read_message ~read_message_type_char:true ~read_payload in
  let%bind () = roundtrip ~write ~read in
  [%expect
    {|
    (message_char (D))
    (length 37)
    (row (("Col 1") ("Col 2") () ("Col 4")))
    |}];
  Deferred.unit
;;

let%expect_test "Command Complete" =
  let write writer = Protocol.Backend.Writer.command_complete writer "DELETE 10" in
  let read_payload ~payload_length:_ iobuf =
    let (response : string) =
      Protocol.Backend.CommandComplete.consume iobuf |> Or_error.ok_exn
    in
    print_s [%message (response : string)]
  in
  let read = read_message ~read_message_type_char:true ~read_payload in
  let%bind () = roundtrip ~write ~read in
  [%expect
    {|
    (message_char (C))
    (length 14)
    (response "DELETE 10")
    |}];
  Deferred.unit
;;

let%expect_test "Notice Response" =
  let write writer =
    Protocol.Backend.Writer.notice_response
      writer
      { error_code = "00000"; all_fields = [ Severity, "LOG" ] }
  in
  let read_payload ~payload_length:_ iobuf =
    let (response : Protocol.Backend.NoticeResponse.t) =
      Protocol.Backend.NoticeResponse.consume iobuf |> Or_error.ok_exn
    in
    print_s [%message (response : Protocol.Backend.NoticeResponse.t)]
  in
  let read = read_message ~read_message_type_char:true ~read_payload in
  let%bind () = roundtrip ~write ~read in
  [%expect
    {|
    (message_char (N))
    (length 17)
    (response ((error_code 00000) (all_fields ((Code 00000) (Severity LOG)))))
    |}];
  Deferred.unit
;;

let%expect_test "Notification Response" =
  let write writer =
    Protocol.Backend.Writer.notification_response
      writer
      { pid = Pid.of_int 10
      ; channel =
          Postgres_async.Private.Types.Notification_channel.of_string "Test Channel"
      ; payload = "Test Payload"
      }
  in
  let read_payload ~payload_length:_ iobuf =
    let (response : Protocol.Backend.NotificationResponse.t) =
      Protocol.Backend.NotificationResponse.consume iobuf |> Or_error.ok_exn
    in
    print_s [%message (response : Protocol.Backend.NotificationResponse.t)]
  in
  let read = read_message ~read_message_type_char:true ~read_payload in
  let%bind () = roundtrip ~write ~read in
  [%expect
    {|
    (message_char (A))
    (length 34)
    (response ((pid 10) (channel "Test Channel") (payload "Test Payload")))
    |}];
  Deferred.unit
;;

let%expect_test "Parameter Description" =
  let write writer =
    Protocol.Backend.Writer.parameter_description writer (Array.of_list [ 12; 23; 1; 5 ])
  in
  let read_payload ~payload_length:_ iobuf =
    let (response : int array) =
      Protocol.Backend.ParameterDescription.consume iobuf |> Or_error.ok_exn
    in
    print_s [%message (response : int array)]
  in
  let read = read_message ~read_message_type_char:true ~read_payload in
  let%bind () = roundtrip ~write ~read in
  [%expect
    {|
    (message_char (t))
    (length 22)
    (response (12 23 1 5))
    |}];
  Deferred.unit
;;

let%expect_test "Parameter Status" =
  let write writer =
    Protocol.Backend.Writer.parameter_status writer { key = "user"; data = "root" }
  in
  let read_payload ~payload_length:_ iobuf =
    let (response : Protocol.Backend.ParameterStatus.t) =
      Protocol.Backend.ParameterStatus.consume iobuf |> Or_error.ok_exn
    in
    print_s [%message (response : Protocol.Backend.ParameterStatus.t)]
  in
  let read = read_message ~read_message_type_char:true ~read_payload in
  let%bind () = roundtrip ~write ~read in
  [%expect
    {|
    (message_char (S))
    (length 14)
    (response ((key user) (data root)))
    |}];
  Deferred.unit
;;

let%expect_test "Query" =
  let write writer =
    Protocol.Frontend.Writer.query writer "SELECT * FROM a; SELECT * FROM a;"
  in
  let read_payload ~payload_length:_ iobuf =
    let (response : string) = Protocol.Frontend.Query.consume iobuf |> Or_error.ok_exn in
    print_s [%message response]
  in
  let read = read_message ~read_message_type_char:true ~read_payload in
  let%bind () = roundtrip ~write ~read in
  [%expect
    {|
    (message_char (Q))
    (length 38)
    "SELECT * FROM a; SELECT * FROM a;"
    |}];
  Deferred.unit
;;

let%test_unit "find_null_exn handles a missing terminator" =
  List.iter [ ""; "not terminated" ] ~f:(fun string ->
    match Protocol.Shared.find_null_exn (Iobuf.of_string string) with
    | exception Failure message ->
      [%test_result: string] message ~expect:"find_null_exn could not find \\x00"
    | _ -> failwith "find_null_exn should reject a missing terminator")
;;
