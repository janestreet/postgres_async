open Core
open Async
module Socket_id = Unique_id.Int ()

let () = Dynamic.set_root Backtrace.elide true
let accept_login = "R\x00\x00\x00\x08\x00\x00\x00\x00Z\x00\x00\x00\x05I"

let with_manual_server ~handle_client ~f:callback =
  let socket_name =
    sprintf
      !"/tmp/.postgres-async-tests-%{Pid}-%{Socket_id}"
      (Unix.getpid ())
      (Socket_id.create ())
  in
  let%bind tcp_server =
    Tcp.Server.create
      ~on_handler_error:`Raise
      (Tcp.Where_to_listen.of_file socket_name)
      (fun _sock reader writer -> handle_client reader writer)
  in
  let where_to_connect = Tcp.Where_to_connect.of_file socket_name in
  let finally () =
    let%bind () = Tcp.Server.close tcp_server in
    let%bind () = Unix.unlink socket_name in
    return ()
  in
  Monitor.protect ~run:`Now ~rest:`Raise ~finally (fun () -> callback where_to_connect)
;;

let startup_message_length = 41

let handle_startup_login_and_close reader writer =
  let process_close_message reader =
    (* Close message is 'X' *)
    match%bind Reader.read_char reader with
    | `Ok 'X' -> Writer.close writer
    | `Ok _ | `Eof ->
      (* Even if we don't get a close message here, we still close the writer. This can
         happen in the "SSL negotiation failure" test below. *)
      Writer.close writer
  in
  (* Client may decide to send startup message or close its connection based on the ssl
     response. *)
  let scratch = Bytes.create startup_message_length in
  match%bind Reader.really_read reader scratch with
  | `Eof _ -> Writer.close writer
  | `Ok ->
    Writer.write writer accept_login;
    process_close_message reader
;;

let crt_file = "server-leaf_certificate.pem"
let key_file = "server-leaf_key.key"

let ssl_server_conf () =
  (* Took these files & config [Async_ssl] (test/lib/ subfolder). *)
  let ca_file = Some "server-leaf_certificate.pem" in
  let ca_path = None in
  Async_ssl.Config.Server.create ~crt_file ~key_file ~ca_file ~ca_path ()
;;

let handle_client ?(force_non_ssl = false) ~ssl_char reader writer =
  (* SSLRequest is always length 8 *)
  let%bind () =
    match%map Reader.peek reader ~len:8 with
    | `Ok _ -> ()
    | `Eof -> failwith "unexpected eof in init message"
  in
  match Reader.bytes_available reader with
  | 8 ->
    let scratch = Bytes.create 8 in
    let%bind () =
      match%bind Reader.really_read reader scratch with
      | `Ok -> return ()
      | `Eof _ -> assert false
    in
    Writer.write_char writer ssl_char;
    (match ssl_char with
     | 'S' ->
       (match force_non_ssl with
        | true -> handle_startup_login_and_close reader writer
        | false ->
          Async_ssl.Tls.wrap_server_connection
            (ssl_server_conf ())
            reader
            writer
            ~f:(fun _conn -> handle_startup_login_and_close))
     | 'N' -> handle_startup_login_and_close reader writer
     | _ ->
       print_endline "closing writer";
       Writer.close writer)
  | 41 ->
    print_endline "skipped ssl";
    handle_startup_login_and_close reader writer
  | unexpected_bytes ->
    [%string "Unexpected initial message length %{unexpected_bytes#Int}"] |> failwith
;;

let ssl_harness () =
  let%bind () =
    (* Postgres refuses to start if ssl files are too broadly permissioned *)
    Deferred.List.iter ~how:`Sequential [ crt_file; key_file ] ~f:(fun file ->
      Unix.chmod file ~perm:0o600)
  in
  let%bind dir = Unix.getcwd () in
  let harness =
    (* this is a blocking operation *)
    Harness.create
      ~extra_server_args:
        [ "-c"
        ; [%string "ssl_cert_file=%{dir}/%{crt_file}"]
        ; "-c"
        ; [%string "ssl_key_file=%{dir}/%{key_file}"]
        ; "-c"
        ; "ssl=on"
        ]
      ()
  in
  return harness
;;

let connect_and_close ~ssl_mode where_to_connect =
  let%bind conn =
    Postgres_async.connect
      ~ssl_mode
      ~server:where_to_connect
      ~user:"postgres"
      ~database:"postgres"
      ()
  in
  match conn with
  | Error error ->
    Expect_test_helpers_core.print_s ~hide_positions:true [%message (error : Error.t)];
    Deferred.unit
  | Ok conn ->
    printf "Connected\n";
    let%bind close_finished = Postgres_async.close conn in
    Or_error.ok_exn close_finished;
    return ()
;;

let%expect_test "SSL negotation failure does not raise" =
  let%bind () =
    with_manual_server
      ~handle_client:(handle_client ~force_non_ssl:true ~ssl_char:'S')
      ~f:(connect_and_close ~ssl_mode:Prefer)
  in
  [%expect
    {|
    (error (
      monitor.ml.Error
      (Ssl_error
        ("error:1408F10B:SSL routines:ssl3_get_record:wrong version number")
        lib/async_ssl/src/ssl.ml:LINE:COL)
      ("<backtrace elided in test>" "Caught by monitor ssl_pipe")))
    |}];
  Deferred.unit
;;

let%expect_test "SSL negotiation: Do not use SSL" =
  let%bind () =
    with_manual_server
      ~handle_client:(handle_client ~ssl_char:'N')
      ~f:(connect_and_close ~ssl_mode:Prefer)
  in
  [%expect {| Connected |}];
  Deferred.unit
;;

let%expect_test "Do not use SSL or connect if server returns unknown char response" =
  let%bind () =
    with_manual_server
      ~handle_client:(handle_client ~ssl_char:'E')
      ~f:(connect_and_close ~ssl_mode:Prefer)
  in
  [%expect
    {|
    closing writer
    (error (
      "Postgres Server indicated it does not understand the SSLRequest message. This may mean that the server is running a very outdated version of postgres, or some other problem may be occurring. You can try to run with ssl_mode = Disable to skip the SSLRequest and use plain TCP."
      (response_char E)))
    |}];
  Deferred.unit
;;

let%expect_test "Use SSL (demonstrates startup,login,and close messages over SSL)" =
  let%bind () =
    with_manual_server
      ~handle_client:(handle_client ~ssl_char:'S')
      ~f:(connect_and_close ~ssl_mode:Prefer)
  in
  [%expect {| Connected |}];
  let%bind () =
    with_manual_server
      ~handle_client:(handle_client ~ssl_char:'S')
      ~f:(connect_and_close ~ssl_mode:Require)
  in
  [%expect {| Connected |}];
  Deferred.unit
;;

let%expect_test "SSL negotiation: Error if SSL is required but not available" =
  let%bind () =
    with_manual_server
      ~handle_client:(handle_client ~ssl_char:'N')
      ~f:(connect_and_close ~ssl_mode:Require)
  in
  [%expect
    {|
    (error
     "Server indicated it cannot use SSL connections, but ssl_mode is set to Require")
    |}];
  let%bind () =
    with_manual_server
      ~handle_client:(handle_client ~ssl_char:'E')
      ~f:(connect_and_close ~ssl_mode:Require)
  in
  [%expect
    {|
    closing writer
    (error (
      "Postgres Server indicated it does not understand the SSLRequest message. This may mean that the server is running a very outdated version of postgres, or some other problem may be occurring. You can try to run with ssl_mode = Disable to skip the SSLRequest and use plain TCP."
      (response_char E)))
    |}];
  Deferred.unit
;;

let%expect_test "sslmode = Disable skips sslrequest" =
  let%bind () =
    with_manual_server
      ~handle_client:(handle_client ~ssl_char:'N')
      ~f:(connect_and_close ~ssl_mode:Disable)
  in
  [%expect
    {|
    skipped ssl
    Connected
    |}];
  Deferred.unit
;;

let%expect_test "Connect to live postgres" =
  let%bind ssl_harness = ssl_harness () in
  let where_to_connect harness =
    let port = Harness.port harness in
    (* SSL connections have to be made over TCP, cannot be made using the Unix Socket *)
    Host_and_port.create ~host:"localhost" ~port |> Tcp.Where_to_connect.of_host_and_port
  in
  let ssl = where_to_connect ssl_harness in
  (* Our pg_hba is set up to still allow TCP connections, so Disable works *)
  let%bind () = connect_and_close ssl ~ssl_mode:Disable in
  [%expect {| Connected |}];
  let%bind () = connect_and_close ssl ~ssl_mode:Prefer in
  [%expect {| Connected |}];
  let%bind () = connect_and_close ssl ~ssl_mode:Require in
  [%expect {| Connected |}];
  let tcp_harness = Harness.create () in
  let tcp = where_to_connect tcp_harness in
  let%bind () = connect_and_close tcp ~ssl_mode:Disable in
  [%expect {| Connected |}];
  let%bind () = connect_and_close tcp ~ssl_mode:Prefer in
  [%expect {| Connected |}];
  let%bind () = connect_and_close tcp ~ssl_mode:Require in
  [%expect
    {|
    (error
     "Server indicated it cannot use SSL connections, but ssl_mode is set to Require")
    |}];
  return ()
;;
