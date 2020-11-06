open! Core
open Async

let harness = lazy (Harness.create ())

let%expect_test "interrupt" =
  let with_dummy_server func =
    let got_connection = Ivar.create () in
    let%bind server =
      Tcp.Server.create
        ~on_handler_error:`Raise
        Tcp.Where_to_listen.of_port_chosen_by_os
        (fun _ reader _ ->
           let%bind _ = Reader.peek reader ~len:1 in
           Ivar.fill got_connection ();
           Reader.drain reader)
    in
    let got_connection = Ivar.read got_connection in
    let addr =
      Host_and_port.create
        ~host:"127.0.0.1"
        ~port:(Tcp.Server.listening_on server)
    in
    let finally () = Tcp.Server.close server in
    Monitor.protect ~run:`Now ~rest:`Raise ~finally (fun () -> func addr ~got_connection)
  in
  let%bind () =
    with_dummy_server (fun addr ~got_connection:_ ->
      (* This is technically racey, because the connect call could in principle complete
         before the [choice] picks the interrupt. But it doesn't. *)
      match%bind
        Postgres_async.with_connection
          ~server:(Tcp.Where_to_connect.of_host_and_port addr)
          ~interrupt:(return ())
          ~database:"dummy"
          ~on_handler_exception:`Raise
          (fun _ -> failwith "connection succeeded!?")
      with
      | Ok (_ : Nothing.t) -> .
      | Error err ->
        print_s [%sexp (err : Error.t)];
        [%expect {|
          (monitor.ml.Error ("connection attempt aborted" 127.0.0.1:PORT)
           ("<backtrace elided in test>" "Caught by monitor try_with_or_error")) |}];
        return ()
    )
  in
  let%bind () =
    with_dummy_server (fun addr ~got_connection ->
      match%bind
        Postgres_async.with_connection
          ~server:(Tcp.Where_to_connect.of_host_and_port addr)
          ~interrupt:got_connection
          ~database:"dummy"
          ~on_handler_exception:`Raise
          (fun _ -> failwith "connection succeeded!?")
      with
      | Ok (_ : Nothing.t) -> .
      | Error err ->
        print_s [%sexp (err : Error.t)];
        [%expect {| "login interrupted" |}];
        return ()
    )
  in
  return ()

let try_login ?(user="postgres") ?password ?(database="postgres") harness =
  let get_user postgres =
    let u_d = Set_once.create () in
    let%bind result =
      Postgres_async.query
        postgres
        "SELECT CURRENT_USER, current_database()"
        ~handle_row:(fun ~column_names:_ ~values ->
          match values with
          | [| Some u; Some d |] -> Set_once.set_exn u_d [%here] (u, d)
          | _ -> failwith "bad query response"
        )
    in
    Or_error.ok_exn result;
    return (Set_once.get_exn u_d [%here])
  in
  let%bind result =
    Postgres_async.with_connection
      ~server:(Harness.where_to_connect harness)
      ~user
      ?password
      ~database
      ~on_handler_exception:`Raise
      get_user
  in
  (* we can't print any more than "login failed" because the error messages are not stable
     wrt. postgres versions. *)
  (match result with
   | Ok (u, d) -> printf "OK; user:%s database:%s\n" u d
   | Error _   -> printf "Login failed\n");
  return ()

let%expect_test "trust auth" =
  let harness = force harness in
  let%bind () = try_login harness in
  [%expect {| OK; user:postgres database:postgres |}];
  let%bind () = try_login harness ~password:"nonsense" in
  [%expect {| OK; user:postgres database:postgres |}];
  return ()

let%expect_test "password authentication" =
  let harness = force harness in
  let%bind () =
    Harness.with_connection_exn harness ~database:"postgres" (fun postgres ->
      let q str =
        let%bind r = Postgres_async.query_expect_no_data postgres str in
        Or_error.ok_exn r;
        return ()
      in
      let%bind () = q "CREATE ROLE role_password_login" in
      let%bind () = q "CREATE ROLE auth_test_1 LOGIN PASSWORD 'test-password'" in
      let%bind () = q "GRANT role_password_login TO auth_test_1" in
      return ()
    )
  in
  [%expect {||}];
  let%bind () = try_login harness ~user:"auth_test_1" ~password:"test-password" in
  [%expect {| OK; user:auth_test_1 database:postgres |}];
  let%bind () = try_login harness ~user:"auth_test_1" ~password:"bad" in
  [%expect {| Login failed |}];
  return ()

let%expect_test "unix sockets & tcp" =
  let harness = force harness in
  let%bind result =
    Postgres_async.with_connection
      ~server:(Tcp.Where_to_connect.of_file (Harness.unix_socket_path harness))
      ~user:"postgres"
      ~database:"postgres"
      ~on_handler_exception:`Raise
      (fun _ -> return ())
  in
  Or_error.ok_exn result;
  let%bind result =
    let hap = Host_and_port.create ~host:"127.0.0.1" ~port:(Harness.port harness) in
    Postgres_async.with_connection
      ~server:(Tcp.Where_to_connect.of_host_and_port hap)
      ~user:"postgres"
      ~database:"postgres"
      ~on_handler_exception:`Raise
      (fun _ -> return ())
  in
  Or_error.ok_exn result;
  return ()

let%expect_test "authentication method we don't support" =
  let%bind tcp_server =
    let handle_client _sock _reader writer =
      Writer.write writer "R\x00\x00\x00\x08\x00\x00\x00\x06";
      Deferred.never ()
    in
    Tcp.Server.create
      ~on_handler_error:`Raise
      Tcp.Where_to_listen.of_port_chosen_by_os
      handle_client
  in
  let where_to_connect =
    let port = Tcp.Server.listening_on tcp_server in
    Tcp.Where_to_connect.of_host_and_port (Host_and_port.create ~host:"localhost" ~port)
  in
  let%bind result =
    Postgres_async.with_connection
      ~server:where_to_connect
      ~user:"postgres"
      ~database:"postgres"
      ~on_handler_exception:`Raise
      (fun _ -> return ())
  in
  print_s [%sexp (result : _ Or_error.t)];
  [%expect {| (Error "Server wants unimplemented auth subtype: SCMCredential") |}];
  return ()

let%expect_test "connection refused" =
  (* bind, but don't listen or accept. *)
  let socket = Core.Unix.socket ~domain:PF_INET ~kind:SOCK_STREAM ~protocol:0 () in
  Core.Unix.bind socket ~addr:(ADDR_INET (Unix.Inet_addr.of_string "0.0.0.0", 0));
  let where_to_connect =
    match Core.Unix.getsockname socket with
    | ADDR_UNIX _ -> assert false
    | ADDR_INET (_, port) ->
      Tcp.Where_to_connect.of_host_and_port (Host_and_port.create ~host:"localhost" ~port)
  in
  let%bind result =
    Postgres_async.with_connection
      ~server:where_to_connect
      ~user:"postgres"
      ~database:"postgres"
      ~on_handler_exception:`Raise
      (fun _ -> return ())
  in
  Core.Unix.close socket;
  print_s [%sexp (result : _ Or_error.t)];
  [%expect {|
    (Error
     (monitor.ml.Error
      (Unix.Unix_error "Connection refused" connect 127.0.0.1:PORT)
      ("<backtrace elided in test>" "Caught by monitor Tcp.close_sock_on_error"))) |}];
  return ()

let%expect_test "graceful close" =
  let harness = force harness in
  let%bind connection =
    Postgres_async.connect ()
      ~server:(Harness.where_to_connect harness)
      ~user:"postgres"
      ~database:"postgres"
  in
  let connection = Or_error.ok_exn connection in
  let%bind result = Postgres_async.close connection in
  print_s [%sexp (result : unit Or_error.t)];
  [%expect {| (Ok ()) |}];
  return ()
