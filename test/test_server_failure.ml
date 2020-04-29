open! Core
open Async
open Expect_test_helpers_core

let harness = lazy (Harness.create ())

let print_or_error or_error =
  let sexp =
    [%sexp (or_error : unit Or_error.t)]
    |> Utils.delete_unstable_bits_of_error
  in
  print_s sexp

let with_connection ?(show_errors=true) ?(database="postgres") func =
  let sexp_of_error error =
    match show_errors with
    | false -> [%sexp "<omitted>"]
    | true -> Utils.delete_unstable_bits_of_error [%sexp (error : Error.t)]
  in
  (* [Postgres_async.with_connection] does not report failure to close the connection
     because we think that users would prefer to get the result of their function instead.
     But it's interesting to print it. *)
  match%bind
    Postgres_async.connect ()
      ~server:(Harness.where_to_connect (force harness))
      ~user:"postgres"
      ~database
  with
  | Error error ->
    print_s [%message "failed to connect" (error : error)];
    return ()
  | Ok postgres ->
    let%bind () = func postgres in
    match%bind Postgres_async.close postgres with
    | Ok () ->
      print_s [%message "closed cleanly"];
      return ()
    | Error error ->
      print_s [%message "failed to close" (error : error)];
      return ()

let%expect_test "terminate backend" =
  (* The easiest way to write this test is to have the backend kill itself. Sadly, if the
     backend is killed while it is executing a query, there's a race: either the write of
     the [Sync] message to fail ("writer failed asynchronously") or the write succeeds
     (getting out the door before the connection is closed) and instead we get unexpected
     EOF on the read that follows (seems to in practice be far more likely).

     Therefore, we can't show the contents of the error messages, sadly. But we can assert
     that we do get errors. *)
  let%bind () =
    with_connection ~show_errors:false (fun postgres ->
      let%bind result =
        Postgres_async.query
          postgres
          "SELECT pg_terminate_backend(pg_backend_pid())"
          ~handle_row:(fun ~column_names:_ ~values:_ -> ())
      in
      print_or_error result;
      let%bind () =
        [%expect {|
          (Error (
            "Error during query execution (despite parsing ok)"
            ((severity FATAL)
             (code     57P01)))) |}]
      in
      let%bind result = Postgres_async.query_expect_no_data postgres "" in
      [%test_pred: unit Or_error.t]
        (fun r ->
           String.is_substring
             (Sexp.to_string [%sexp (r : unit Or_error.t)])
             ~substring:"query issued against previously-failed connection")
        result;
      return ()
    )
  in
  let%bind () =
    [%expect {| ("failed to close" (error <omitted>)) |}]
  in
  (* Here's a test where our backend is externally terminated while we're idle; a little
     tricky to get right, but this one is actually deterministic (because we're not trying
     to write at the same time as the connection is closed on us). *)
  let%bind () =
    with_connection (fun postgres ->
      let backend_pid = Set_once.create () in
      let%bind result =
        Postgres_async.query
          postgres
          "SELECT pg_backend_pid()"
          ~handle_row:(fun ~column_names:_ ~values ->
            match values with
            | [|Some p|] -> Set_once.set_exn backend_pid [%here] p
            | _ -> assert false
          )
      in
      Or_error.ok_exn result;
      let backend_pid = Set_once.get_exn backend_pid [%here] in
      let%bind () =
        Harness.with_connection_exn (force harness) ~database:"postgres" (fun postgres2 ->
          let%bind result =
            Postgres_async.query
              postgres2
              "SELECT pg_terminate_backend($1)"
              ~parameters:[|Some backend_pid|]
              ~handle_row:(fun ~column_names:_ ~values:_ -> ())
          in
          Or_error.ok_exn result;
          return ()
        )
      in
      let%bind result = Postgres_async.close_finished postgres in
      print_or_error result;
      let%bind () =
        [%expect {|
          (Error (
            "ErrorResponse received asynchronously, assuming connection is dead"
            ((severity FATAL)
             (code     57P01)))) |}]
      in
      let%bind result =
        Postgres_async.query_expect_no_data postgres ""
      in
      print_or_error result;
      let%bind () =
        [%expect {|
          (Error (
            "query issued against previously-failed connection"
            (original_error (
              "ErrorResponse received asynchronously, assuming connection is dead"
              ((severity FATAL)
               (code     57P01)))))) |}]
      in
      return ()
    )
  in
  let%bind () =
    [%expect {|
      ("failed to close" (
        error (
          "ErrorResponse received asynchronously, assuming connection is dead"
          ((severity FATAL)
           (code     57P01))))) |}]
  in
  return ()

let accept_login = "R\x00\x00\x00\x08\x00\x00\x00\x00Z\x00\x00\x00\x05I"

module Socket_id = Unique_id.Int ()

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
  Monitor.protect ~finally (fun () -> callback where_to_connect)

let send_eof writer =
  let%bind () = Writer.flushed writer in
  Fd.syscall_exn (Writer.fd writer) (Core.Unix.shutdown ~mode:SHUTDOWN_SEND);
  return ()

let%expect_test "invaild messages during login" =
  let try_connect response =
    with_manual_server
      ~handle_client:(fun reader writer ->
        (* wait for the startup message. *)
        let%bind _ = Reader.read_char reader in
        (* pre-prepared binary response *)
        Writer.write writer response;
        let%bind () = send_eof writer in
        Deferred.never ()
      )
      ~f:(fun where_to_connect ->
        let%bind result =
          Postgres_async.with_connection
            ~server:where_to_connect
            ~user:"postgres"
            ~database:"postgres"
            (fun _ -> return ())
        in
        print_s [%sexp (result : _ Or_error.t)];
        return ()
      )
  in
  (* demonstrate that our TCP server works: *)
  let%bind () = try_connect accept_login in
  let%bind () =
    [%expect {| (Ok _) |}]
  in
  (* nonsense message length. *)
  let%bind () = try_connect "R\x00\x00\x00\x03" in
  let%bind () =
    [%expect {|
      (Error ("Nonsense message length in header" 4)) |}]
  in
  (* authentication method we don't recognise *)
  let%bind () = try_connect "R\x00\x00\x00\x08\x00\x00\x00\xFF" in
  let%bind () =
    [%expect {|
      (Error (
        "Failed to parse AuthenticationRequest" (
          exn ("AuthenticationRequest unrecognised type" (other 255))))) |}]
  in
  (* message type we don't recognise *)
  let%bind () = try_connect "x\x00\x00\x00\x04" in
  let%bind () =
    [%expect {|
      (Error ("Unrecognised message type character" (other x))) |}]
  in
  (* message type we recognise but don't expect *)
  let%bind () = try_connect "s\x00\x00\x00\x04" in
  let%bind () =
    [%expect {|
      (Error (
        "Unexpected message type"
        (msg_type PortalSuspended)
        (state    "logging in"))) |}]
  in
  (* very long message *)
  let%bind () =
    try_connect (
      "R\x08\x00\x00\x00"
      ^ String.init (1024 * 1024) ~f:(fun _ -> '\x00')
    )
  in
  let%bind () =
    [%expect {|
      (Error ("Message too long" (iobuf_length 1048581))) |}]
  in
  (* message truncated by EOF *)
  let%bind () = try_connect "R\x00\x00\x00\x05" in
  let%bind () =
    [%expect {|
      (Error ("Unexpected EOF" (unconsumed_bytes 5))) |}]
  in
  return ()

let%expect_test "invalid messages during query_expect_no_data" =
  let try_query ?(show_second_result=false) query_response =
    let handle_client reader writer =
      (* wait for the startup message. *)
      let scratch = Bytes.create 41 in
      let%bind () =
        match%bind Reader.really_read reader scratch with
        | `Ok -> return ()
        | `Eof _ -> assert false
      in
      let scratch = Bytes.to_string scratch in
      (* startup messages end with two nulls *)
      assert (Char.equal scratch.[39] '\x00' && Char.equal scratch.[40] '\x00');
      Writer.write writer accept_login;
      (* wait for the query to start executing. *)
      let%bind parse =
        match%bind Reader.read_char reader with
        | `Ok c -> return c
        | `Eof -> assert false
      in
      (* the first message should be a parse. *)
      assert (Char.equal parse 'P');
      Writer.write writer query_response;
      let%bind () = send_eof writer in
      Deferred.never ()
    in
    let send_queries postgres =
      let handle_row ~column_names ~values =
        let sexp_of_sopt =
          function
          | None -> [%sexp "<none>"]
          | Some s -> [%sexp (s : string)]
        in
        print_s [%message "row" (column_names : string array) (values : sopt array)]
      in
      let%bind r1 = Postgres_async.query postgres "<dummy>" ~handle_row in
      print_s ~hide_positions:true [%message (r1 : _ Or_error.t)];
      let%bind () =
        match r1 with
        | Ok () -> return ()
        | Error _ ->
          match%bind Postgres_async.close_finished postgres with
          | Ok _ -> failwith "close_finished returned Ok _ ?"
          | Error _ ->
            printf "close_finished is determined with an error\n";
            match%bind Postgres_async.query postgres "<dummy>" ~handle_row with
            | Ok _ -> failwith "second query succeeded?"
            | Error _ as r2 ->
              (match show_second_result with
               | false -> ()
               | true -> print_s ~hide_positions:true [%message (r2 : _ Or_error.t)]);
              return ()
      in
      return ()
    in
    with_manual_server ~handle_client ~f:(fun where_to_connect ->
      let%bind outer_result =
        Postgres_async.with_connection
          ~server:where_to_connect
          ~user:"postgres"
          ~database:"postgres"
          send_queries
      in
      print_s [%message (outer_result : _ Or_error.t)];
      return ()
    )
  in
  (* demonstrate that our TCP server works: *)
  let parsecomplete = "1\x00\x00\x00\x04" in
  let bindcomplete = "2\x00\x00\x00\x04" in
  let nodata = "n\x00\x00\x00\x04" in
  let emptyqueryresponse = "I\x00\x00\x00\x04" in
  let rowdescription = "T\x00\x00\x00\x06\x00\x00" in
  let commandcomplete = "C\x00\x00\x00\x06_\x00" in
  let readyforquery = "Z\x00\x00\x00\x05I" in
  let%bind () =
    try_query ~show_second_result:true (
      parsecomplete
      ^ bindcomplete
      ^ nodata
      ^ emptyqueryresponse
      ^ readyforquery
    )
  in
  let%bind () =
    [%expect {|
      (r1 (Ok _))
      (outer_result (Ok _)) |}]
  in
  (* unexpected or unrecognised message types at various stages. *)
  let%bind () = try_query "x\x00\x00\x00\x04" in
  let%bind () =
    [%expect {|
      (r1 (Error ("Unrecognised message type character" (other x))))
      close_finished is determined with an error
      (outer_result (Ok _)) |}]
  in
  let%bind () = try_query (parsecomplete ^ "s\x00\x00\x00\x04") in
  let%bind () =
    [%expect {|
      (r1 (
        Error (
          "Unexpected message type"
          (msg_type PortalSuspended)
          (state    Binding))))
      close_finished is determined with an error
      (outer_result (Ok _)) |}]
  in
  let%bind () = try_query (parsecomplete ^ bindcomplete ^ "x\x00\x00\x00\x04") in
  let%bind () =
    [%expect {|
      (r1 (Error ("Unrecognised message type character" (other x))))
      close_finished is determined with an error
      (outer_result (Ok _)) |}]
  in
  let%bind () =
    try_query (
      parsecomplete
      ^ bindcomplete
      ^ rowdescription
      ^ "s\x00\x00\x00\x04"
    )
  in
  let%bind () =
    [%expect {|
      (r1 (
        Error (
          "Unexpected message type"
          (msg_type PortalSuspended)
          (state    "reading DataRows"))))
      close_finished is determined with an error
      (outer_result (Ok _)) |}]
  in
  (* second query fails cleanly too: *)
  let%bind () = try_query "x\x00\x00\x00\x04" ~show_second_result:true in
  let%bind () =
    [%expect {|
      (r1 (Error ("Unrecognised message type character" (other x))))
      close_finished is determined with an error
      (r2 (
        Error (
          "query issued against previously-failed connection"
          (original_error ("Unrecognised message type character" (other x))))))
      (outer_result (Ok _)) |}]
  in
  (* rowdescription with junk in *)
  let%bind () =
    try_query (
      parsecomplete
      ^ bindcomplete
      ^ "T\x00\x00\x00\x06\x0f\xff"
    )
  in
  let%bind () =
    [%expect {|
      (r1 (
        Error (
          "Failed to parse RowDescription" (
            exn (
              "Iobuf got invalid range" (
                ((pos 0)
                 (len 1))
                ((buf    <opaque>)
                 (lo_min 0)
                 (lo     17)
                 (hi     17)
                 (hi_max 17))))))))
      close_finished is determined with an error
      (outer_result (Ok _)) |}]
  in
  (* datarow with mismatched number of fields vs. row desc. *)
  let%bind () =
    let z18 = String.init 18 ~f:(fun _ -> '\x00') in
    try_query (
      parsecomplete
      ^ bindcomplete
      ^ "T\x00\x00\x00\x2e\x00\x02A\x00" ^ z18 ^ "B\x00" ^ z18
      ^ "D\x00\x00\x00\x0b\x00\x01\x00\x00\x00\x01a"
      ^ commandcomplete
      ^ readyforquery
    )
  in
  let%bind () =
    [%expect {|
      (r1 (
        Error (
          "number of columns in DataRow message did not match RowDescription"
          (column_names (A B))
          (values ((a))))))
      close_finished is determined with an error
      (outer_result (Ok _)) |}]
  in
  (* very long datarow *)
  let%bind () =
    try_query (
      parsecomplete
      ^ bindcomplete
      ^ rowdescription
      ^ "D\x08\x00\x00\x00" ^ String.init (1024 * 1024) ~f:(fun _ -> '\x00')
    )
  in
  let%bind () =
    [%expect {|
      (r1 (Error ("Message too long" (iobuf_length 1048581))))
      close_finished is determined with an error
      (outer_result (Ok _)) |}]
  in
  (* message truncated by EOF *)
  let%bind () =
    try_query (
      parsecomplete
      ^ bindcomplete
      ^ rowdescription
      ^ "D\x00\x00\x00\x10"
    )
  in
  let%bind () =
    [%expect {|
      (r1 (Error ("Unexpected EOF" (unconsumed_bytes 5))))
      close_finished is determined with an error
      (outer_result (Ok _)) |}]
  in
  return ()

let%expect_test "asynchronous EOF" =
  let send_eof_now = Ivar.create () in
  let handle_client reader writer =
    (* wait for the startup message. *)
    let%bind _ = Reader.read_char reader in
    (* accept login *)
    Writer.write writer accept_login;
    (* send EOF *)
    let%bind () = Ivar.read send_eof_now in
    let%bind () = send_eof writer in
    Deferred.never ()
  in
  with_manual_server ~handle_client ~f:(fun where_to_connect ->
    let handle_connection postgres =
      let close_finished_deferred = Postgres_async.close_finished postgres in
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      printf "in connection handler\n";
      print_s [%message (close_finished_deferred : unit Or_error.t Deferred.t)];
      printf "now sending EOF\n";
      Ivar.fill send_eof_now ();
      (* postgres_async should notice asynchronously, without issuing a query. *)
      let%bind close_finished  = close_finished_deferred in
      print_s [%message (close_finished : unit Or_error.t)];
      return ()
    in
    let%bind outer_result =
      Postgres_async.with_connection
        ~server:where_to_connect
        ~user:"postgres"
        ~database:"postgres"
        handle_connection
    in
    print_s [%message (outer_result : _ Or_error.t)];
    [%expect {|
      in connection handler
      (close_finished_deferred Empty)
      now sending EOF
      (close_finished (Error "Unexpected EOF (no unconsumed messages)"))
      (outer_result (Ok _)) |}]
  )

let%expect_test "asynchronous writer failure during login" =
  (* note that SHUTDOWN_RECV doesn't work on TCP; fortunately we have unix sockets. *)
  with_manual_server
    ~handle_client:(fun reader writer ->
      (* wait for the startup message. *)
      let%bind _ = Reader.read_char reader in
      (* shutdown read. *)
      Fd.syscall_exn (Reader.fd reader) (Core.Unix.shutdown ~mode:SHUTDOWN_RECEIVE);
      (* ask for a password. *)
      Writer.write writer "R\x00\x00\x00\x0c\x00\x00\x00\x05salt";
      Deferred.never ()
    )
    ~f:(fun where_to_connect ->
      let%bind result =
        Postgres_async.connect ()
          ~server:where_to_connect
          ~user:"postgres"
          ~database:"postgres"
          ~password:"postgres"
      in
      let result = Utils.delete_unstable_bits_of_error [%sexp (result : _ Or_error.t)] in
      print_s result;
      [%expect {|
        (Error (
          "Writer failed asynchronously" (
            exn (
              monitor.ml.Error
              ("Writer error from inner_monitor"
                (Unix.Unix_error "Broken pipe" writev_assume_fd_is_nonblocking "")
                <omitted>)
              ("Caught by monitor Writer.monitor"))))) |}]
    )

let%expect_test "asynchronous writer failure during query" =
  with_manual_server
    ~handle_client:(fun reader writer ->
      (* login. *)
      let%bind _ = Reader.read_char reader in
      Writer.write writer accept_login;
      (* shutdown read before the query is sent. *)
      Fd.syscall_exn (Reader.fd reader) (Core.Unix.shutdown ~mode:SHUTDOWN_RECEIVE);
      Deferred.never ()
    )
    ~f:(fun where_to_connect ->
      let handle_connection postgres =
        printf "connected\n";
        let%bind result = Postgres_async.query_expect_no_data postgres "<dummy>" in
        let result = [%message (result : _ Or_error.t)] in
        print_s (Utils.delete_unstable_bits_of_error result);
        return ()
      in
      let%bind outer_result =
        Postgres_async.with_connection
          ~server:where_to_connect
          ~user:"postgres"
          ~database:"postgres"
          ~password:"postgres"
          handle_connection
      in
      print_s [%message (outer_result : unit Or_error.t)];
      [%expect {|
        connected
        (result (
          Error (
            "Writer failed asynchronously" (
              exn (
                monitor.ml.Error
                ("Writer error from inner_monitor"
                  (Unix.Unix_error "Broken pipe" writev_assume_fd_is_nonblocking "")
                  <omitted>)
                ("Caught by monitor Writer.monitor"))))))
        (outer_result (Ok ())) |}]
    )
