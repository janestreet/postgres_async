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
    Monitor.protect ~finally (fun () -> func addr ~got_connection)
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
          (fun _ -> failwith "connection succeeded!?")
      with
      | Ok (_ : Nothing.t) -> .
      | Error err ->
        print_s [%sexp (err : Error.t)];
        [%expect {|
          (monitor.ml.Error
           ("connection attempt aborted" 127.0.0.1:PORT
            lib/async_unix/src/tcp.ml:122:11)
           ("<backtrace elided in test>" "Caught by monitor try_with_or_error")) |}]
    )
  in
  let%bind () =
    with_dummy_server (fun addr ~got_connection ->
      match%bind
        Postgres_async.with_connection
          ~server:(Tcp.Where_to_connect.of_host_and_port addr)
          ~interrupt:got_connection
          ~database:"dummy"
          (fun _ -> failwith "connection succeeded!?")
      with
      | Ok (_ : Nothing.t) -> .
      | Error err ->
        print_s [%sexp (err : Error.t)];
        [%expect {| "login interrupted" |}]
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
  let%bind () = [%expect {| OK; user:postgres database:postgres |}] in
  let%bind () = try_login harness ~password:"nonsense" in
  let%bind () = [%expect {| OK; user:postgres database:postgres |}] in
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
  let%bind () = [%expect {||}] in
  let%bind () = try_login harness ~user:"auth_test_1" ~password:"test-password" in
  let%bind () = [%expect {| OK; user:auth_test_1 database:postgres |}] in
  let%bind () = try_login harness ~user:"auth_test_1" ~password:"bad" in
  let%bind () = [%expect {| Login failed |}] in
  return ()

let%expect_test "unix sockets & tcp" =
  let harness = force harness in
  let%bind result =
    Postgres_async.with_connection
      ~server:(Tcp.Where_to_connect.of_file (Harness.unix_socket_path harness))
      ~user:"postgres"
      ~database:"postgres"
      (fun _ -> return ())
  in
  Or_error.ok_exn result;
  let%bind result =
    let hap = Host_and_port.create ~host:"127.0.0.1" ~port:(Harness.port harness) in
    Postgres_async.with_connection
      ~server:(Tcp.Where_to_connect.of_host_and_port hap)
      ~user:"postgres"
      ~database:"postgres"
      (fun _ -> return ())
  in
  Or_error.ok_exn result;
  return ()
