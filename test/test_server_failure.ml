open! Core
open Async
open Expect_test_helpers_core

let harness = lazy (Harness.create ())

let print_or_error or_error =
  let sexp =
    [%sexp (or_error : unit Or_error.t)] |> Utils.delete_unstable_bits_of_error
  in
  print_s sexp
;;

let with_connection ?(show_errors = true) ?(database = "postgres") func =
  let sexp_of_error error =
    match show_errors with
    | false -> [%sexp "<omitted>"]
    | true -> Utils.delete_unstable_bits_of_error [%sexp (error : Error.t)]
  in
  (* [Postgres_async.with_connection] does not report failure to close the connection
     because we think that users would prefer to get the result of their function instead.
     But it's interesting to print it. *)
  match%bind
    Postgres_async.connect
      ()
      ~server:(Harness.where_to_connect (force harness))
      ~user:"postgres"
      ~database
  with
  | Error error ->
    print_s [%message "failed to connect" (error : error)];
    return ()
  | Ok postgres ->
    let%bind () = func postgres in
    (match%bind Postgres_async.close postgres with
     | Ok () ->
       print_s [%message "closed cleanly"];
       return ()
     | Error error ->
       print_s [%message "failed to close" (error : error)];
       return ())
;;

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
      [%expect
        {|
        (Error (
          (query "SELECT pg_terminate_backend(pg_backend_pid())")
          ("Error during query execution (despite parsing ok)"
           ((Severity FATAL)
            (Code     57P01)))))
        |}];
      let%bind result = Postgres_async.query_expect_no_data postgres "" in
      [%test_pred: unit Or_error.t]
        (fun r ->
          String.is_substring
            (Sexp.to_string [%sexp (r : unit Or_error.t)])
            ~substring:"query issued against previously-failed connection")
        result;
      return ())
  in
  [%expect {| ("failed to close" (error <omitted>)) |}];
  (* Here's a test where our backend is externally terminated while we're idle; a little
     tricky to get right, but this one is actually deterministic (because we're not trying
     to write at the same time as the connection is closed on us). *)
  let%bind () =
    with_connection (fun postgres ->
      let%bind backend_pid = Utils.pg_backend_pid postgres in
      let%bind () =
        Harness.with_connection_exn (force harness) ~database:"postgres" (fun postgres2 ->
          let%bind result =
            Postgres_async.query
              postgres2
              "SELECT pg_terminate_backend($1)"
              ~parameters:[| Some backend_pid |]
              ~handle_row:(fun ~column_names:_ ~values:_ -> ())
          in
          Or_error.ok_exn result;
          return ())
      in
      let%bind result = Postgres_async.close_finished postgres in
      print_or_error result;
      [%expect
        {|
        (Error (
          "ErrorResponse received asynchronously, assuming connection is dead"
          ((Severity FATAL)
           (Code     57P01))))
        |}];
      let%bind result = Postgres_async.query_expect_no_data postgres "" in
      print_or_error result;
      [%expect
        {|
        (Error (
          (query "")
          ("query issued against previously-failed connection"
           (original_error (
             "ErrorResponse received asynchronously, assuming connection is dead"
             ((Severity FATAL)
              (Code     57P01)))))))
        |}];
      return ())
  in
  [%expect
    {|
    ("failed to close" (
      error (
        "ErrorResponse received asynchronously, assuming connection is dead"
        ((Severity FATAL)
         (Code     57P01)))))
    |}];
  return ()
;;

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
  Monitor.protect ~run:`Now ~rest:`Raise ~finally (fun () -> callback where_to_connect)
;;

let send_eof writer =
  let%bind () = Writer.flushed writer in
  Fd.syscall_exn (Writer.fd writer) (Core_unix.shutdown ~mode:SHUTDOWN_SEND);
  return ()
;;

let read_frontend_message reader =
  let%bind message_type =
    match%map Reader.read_char reader with
    | `Ok char -> char
    | `Eof -> failwith "unexpected EOF reading frontend message type"
  in
  let length_bytes = Bytes.create 4 in
  let%bind () =
    match%map Reader.really_read reader length_bytes with
    | `Ok -> ()
    | `Eof _ -> failwith "unexpected EOF reading frontend message length"
  in
  let byte index = Char.to_int (Bytes.get length_bytes index) in
  let length = (byte 0 lsl 24) lor (byte 1 lsl 16) lor (byte 2 lsl 8) lor byte 3 in
  let payload = Bytes.create (length - 4) in
  let%map () =
    match%map Reader.really_read reader payload with
    | `Ok -> ()
    | `Eof _ -> failwith "unexpected EOF reading frontend message payload"
  in
  message_type
;;

let rec read_frontend_messages_through reader ~last_message_type reversed_message_types =
  let%bind message_type = read_frontend_message reader in
  let reversed_message_types = message_type :: reversed_message_types in
  match Char.equal message_type last_message_type with
  | true -> return (List.rev reversed_message_types)
  | false ->
    read_frontend_messages_through reader ~last_message_type reversed_message_types
;;

let row_description_message format =
  let iobuf = Iobuf.create ~len:27 in
  Iobuf.Fill.char iobuf 'T';
  Iobuf.Fill.int32_be_trunc iobuf 26;
  Iobuf.Fill.int16_be_trunc iobuf 1;
  Iobuf.Fill.stringo iobuf "b";
  Iobuf.Fill.char iobuf '\000';
  Iobuf.Fill.int32_be_trunc iobuf 0;
  Iobuf.Fill.int16_be_trunc iobuf 0;
  Iobuf.Fill.int32_be_trunc iobuf 17;
  Iobuf.Fill.int16_be_trunc iobuf (-1);
  Iobuf.Fill.int32_be_trunc iobuf (-1);
  Iobuf.Fill.int16_be_trunc iobuf format;
  Iobuf.flip_lo iobuf;
  Iobuf.Consume.stringo iobuf
;;

let%expect_test "default and mixed-format frontend message sequences" =
  let parse_complete = "1\x00\x00\x00\x04" in
  let bind_complete = "2\x00\x00\x00\x04" in
  let parameter_description = "t\x00\x00\x00\x06\x00\x00" in
  let text_row_description = row_description_message 0 in
  let binary_row_description = row_description_message 1 in
  let command_complete = "C\x00\x00\x00\x06_\x00" in
  let ready_for_query = "Z\x00\x00\x00\x05I" in
  let handle_client reader writer =
    let startup_message = Bytes.create 41 in
    let%bind () =
      match%map Reader.really_read reader startup_message with
      | `Ok -> ()
      | `Eof _ -> failwith "unexpected EOF reading startup message"
    in
    Writer.write writer accept_login;
    let%bind default_messages =
      read_frontend_messages_through reader ~last_message_type:'S' []
    in
    print_s [%message (default_messages : char list)];
    Writer.write
      writer
      (parse_complete
       ^ bind_complete
       ^ text_row_description
       ^ command_complete
       ^ ready_for_query);
    let%bind describe_messages =
      read_frontend_messages_through reader ~last_message_type:'H' []
    in
    print_s [%message (describe_messages : char list)];
    Writer.write writer (parse_complete ^ parameter_description ^ text_row_description);
    let%bind execute_messages =
      read_frontend_messages_through reader ~last_message_type:'S' []
    in
    print_s [%message (execute_messages : char list)];
    Writer.write
      writer
      (bind_complete ^ binary_row_description ^ command_complete ^ ready_for_query);
    let%map (_ : [ `Eof | `Ok of char ]) = Reader.read_char reader in
    ()
  in
  let%bind () =
    with_manual_server ~handle_client ~f:(fun where_to_connect ->
      Postgres_async.with_connection
        ~server:where_to_connect
        ~user:"postgres"
        ~database:"postgres"
        ~on_handler_exception:`Raise
        (fun postgres ->
           let%bind () =
             Postgres_async.query_zero_copy postgres "<default>" ~handle_row:(fun _ -> ())
             >>| ok_exn
           in
           let result_format_for_oid oid : Postgres_async.Types.Format.t =
             print_s [%message (oid : int)];
             Binary
           in
           Postgres_async.query_zero_copy
             postgres
             ~result_format_for_oid
             ~handle_columns:(fun columns ->
               let columns =
                 Iarray.map columns ~f:(fun column ->
                   ( Postgres_async.Column_metadata.name column
                   , Postgres_async.Column_metadata.format column
                   , Postgres_async.Column_metadata.pg_type_oid column ))
               in
               print_s
                 [%sexp (columns : (string * Postgres_async.Types.Format.t * int) iarray)])
             "<mixed>"
             ~handle_row:(fun _ -> ())
           >>| ok_exn)
      >>| ok_exn)
  in
  [%expect
    {|
    (default_messages (P B D E S))
    (describe_messages (P D H))
    (oid 17)
    (execute_messages (B D E S))
    ((b Binary 17))
    |}];
  return ()
;;

let%expect_test "phase-one error sends Sync before connection reuse" =
  let parse_complete = "1\x00\x00\x00\x04" in
  let bind_complete = "2\x00\x00\x00\x04" in
  let parameter_description = "t\x00\x00\x00\x06\x00\x00" in
  let text_row_description = row_description_message 0 in
  let command_complete = "C\x00\x00\x00\x06_\x00" in
  let ready_for_query = "Z\x00\x00\x00\x05I" in
  let handle_client reader writer =
    let startup_message = Bytes.create 41 in
    let%bind () =
      match%map Reader.really_read reader startup_message with
      | `Ok -> ()
      | `Eof _ -> failwith "unexpected EOF reading startup message"
    in
    Writer.write writer accept_login;
    let%bind describe_messages =
      read_frontend_messages_through reader ~last_message_type:'H' []
    in
    print_s [%message (describe_messages : char list)];
    Writer.write writer (parse_complete ^ parameter_description);
    Postgres_async.Private.Protocol.Backend.Writer.error_response
      writer
      { error_code = "40001"
      ; all_fields = [ Severity, "ERROR"; Message, "error describing statement" ]
      };
    let%bind recovery_messages =
      read_frontend_messages_through reader ~last_message_type:'S' []
    in
    print_s [%message (recovery_messages : char list)];
    Writer.write writer ready_for_query;
    let%bind reuse_messages =
      read_frontend_messages_through reader ~last_message_type:'S' []
    in
    print_s [%message (reuse_messages : char list)];
    Writer.write
      writer
      (parse_complete
       ^ bind_complete
       ^ text_row_description
       ^ command_complete
       ^ ready_for_query);
    let%map (_ : [ `Eof | `Ok of char ]) = Reader.read_char reader in
    ()
  in
  let%bind () =
    with_manual_server ~handle_client ~f:(fun where_to_connect ->
      Postgres_async.with_connection
        ~server:where_to_connect
        ~user:"postgres"
        ~database:"postgres"
        ~on_handler_exception:`Raise
        (fun postgres ->
           let%bind result =
             Postgres_async.query_zero_copy
               postgres
               ~result_format_for_oid:(fun (_ : int) -> Binary)
               "<phase-one-error>"
               ~handle_row:(fun _ -> ())
           in
           (match result with
            | Ok () -> print_endline "unexpected success"
            | Error error ->
              let error = Utils.delete_unstable_bits_of_error (Error.sexp_of_t error) in
              print_s [%message "Error" ~_:(error : Sexp.t)]);
           Postgres_async.query_zero_copy postgres "<reuse>" ~handle_row:(fun _ -> ())
           >>| ok_exn)
      >>| ok_exn)
  in
  [%expect
    {|
    (describe_messages (P D H))
    (recovery_messages (S))
    (Error (
      (query <phase-one-error>)
      ("Postgres Server Error (state=Describing_statement)" ((Code 40001)))))
    (reuse_messages (P B D E S))
    |}];
  return ()
;;

let%expect_test "fatal error without ReadyForQuery is preserved" =
  let parse_complete = "1\x00\x00\x00\x04" in
  let bind_complete = "2\x00\x00\x00\x04" in
  let command_complete = "C\x00\x00\x00\x06_\x00" in
  let handle_client reader writer =
    let startup_message = Bytes.create 41 in
    let%bind () =
      match%map Reader.really_read reader startup_message with
      | `Ok -> ()
      | `Eof _ -> failwith "unexpected EOF reading startup message"
    in
    Writer.write writer accept_login;
    let%bind (_ : char list) =
      read_frontend_messages_through reader ~last_message_type:'S' []
    in
    Writer.write
      writer
      (parse_complete ^ bind_complete ^ row_description_message 0 ^ command_complete);
    Postgres_async.Private.Protocol.Backend.Writer.error_response
      writer
      { error_code = "57P01"
      ; all_fields = [ Severity, "FATAL"; Message, "terminating connection" ]
      };
    let%bind () = send_eof writer in
    Deferred.never ()
  in
  let%bind () =
    with_manual_server ~handle_client ~f:(fun where_to_connect ->
      Postgres_async.with_connection
        ~server:where_to_connect
        ~user:"postgres"
        ~database:"postgres"
        ~on_handler_exception:`Raise
        (fun postgres ->
           let%map result =
             Postgres_async.query_zero_copy postgres "<fatal>" ~handle_row:(fun _ -> ())
           in
           match result with
           | Ok () -> print_endline "unexpected success"
           | Error error ->
             let error = Utils.delete_unstable_bits_of_error (Error.sexp_of_t error) in
             print_s [%message "Error" ~_:(error : Sexp.t)])
      >>| ok_exn)
  in
  [%expect
    {|
    (Error (
      (query <fatal>)
      ("Error waiting for ReadyForQuery (after query execution finished)"
       ((Code     57P01)
        (Severity FATAL)))))
    |}];
  return ()
;;

let%expect_test "invaild messages during login" =
  let try_connect ?(send_eof_after_response = true) response =
    with_manual_server
      ~handle_client:(fun reader writer ->
        (* wait for the startup message. *)
        let%bind _ = Reader.read_char reader in
        (* pre-prepared binary response *)
        Writer.write writer response;
        let%bind () =
          match send_eof_after_response with
          | true -> send_eof writer
          | false -> return ()
        in
        Deferred.never ())
      ~f:(fun where_to_connect ->
        let%bind result =
          Postgres_async.with_connection
            ~server:where_to_connect
            ~user:"postgres"
            ~database:"postgres"
            ~on_handler_exception:`Raise
            (fun _ -> return ())
        in
        print_s ~hide_positions:true [%sexp (result : _ Or_error.t)];
        return ())
  in
  (* demonstrate that our TCP server works: *)
  let%bind () = try_connect accept_login in
  [%expect {| (Ok _) |}];
  (* nonsense message length. *)
  let%bind () = try_connect "R\x00\x00\x00\x03" in
  [%expect {| (Error ("Nonsense message length in header" 4)) |}];
  (* authentication method we don't recognise *)
  let%bind () = try_connect "R\x00\x00\x00\x08\x00\x00\x00\xFF" in
  [%expect
    {|
    (Error (
      "Failed to parse AuthenticationRequest" (
        exn ("AuthenticationRequest unrecognised type" (other 255)))))
    |}];
  (* message type we don't recognise *)
  let%bind () = try_connect "x\x00\x00\x00\x04" in
  [%expect {| (Error ("Unrecognised message type character" (other x))) |}];
  (* message type we recognise but don't expect *)
  let%bind () = try_connect "s\x00\x00\x00\x04" in
  [%expect
    {|
    (Error (
      "Unexpected message type"
      (msg_type PortalSuspended)
      (state    "logging in")
      (here lib/postgres_async/src/postgres_async.ml:LINE:COL)))
    |}];
  (* very long message *)
  let%bind () =
    try_connect ~send_eof_after_response:false "R\x40\x00\x00\x04 blah blah blah"
  in
  [%expect {| (Error ("Message too long" (message_length 1073741829))) |}];
  (* message truncated by EOF *)
  let%bind () = try_connect "R\x00\x00\x00\x05" in
  [%expect {| (Error ("Unexpected EOF" (unconsumed_bytes 5))) |}];
  return ()
;;

let%expect_test "GSS auth request without token reports authorization error" =
  let gss_auth_request = "R\x00\x00\x00\x08\x00\x00\x00\x07" in
  let%bind () =
    with_manual_server
      ~handle_client:(fun reader writer ->
        (* wait for the startup message. *)
        let%bind _ = Reader.read_char reader in
        Writer.write writer gss_auth_request;
        Deferred.never ())
      ~f:(fun where_to_connect ->
        let%bind result =
          Postgres_async.Expert.connect
            ()
            ~server:where_to_connect
            ~user:"postgres"
            ~database:"postgres"
        in
        (match result with
         | Ok _ -> raise_s [%message "Connected unexpectedly"]
         | Error error ->
           let error_code = Postgres_async.Pgasync_error.postgres_error_code error in
           print_s
             [%message
               (error_code : string option) (error : Postgres_async.Pgasync_error.t)]);
        return ())
  in
  [%expect
    {|
    ((error_code (28000))
     (error "Server requested GSS auth, but no gss_krb_token was provided"))
    |}];
  return ()
;;

let%expect_test "invalid messages during query_expect_no_data" =
  let try_query
    ?(show_second_result = false)
    ?(send_eof_after_response = true)
    query_response
    =
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
      let%bind () =
        match send_eof_after_response with
        | true -> send_eof writer
        | false -> return ()
      in
      Deferred.never ()
    in
    let send_queries postgres =
      let handle_row ~column_names ~values =
        let sexp_of_sopt = function
          | None -> [%sexp "<none>"]
          | Some s -> [%sexp (s : string)]
        in
        print_s [%message "row" (column_names : string iarray) (values : sopt iarray)]
      in
      let%bind r1 = Postgres_async.query postgres "<dummy>" ~handle_row in
      print_s ~hide_positions:true [%message (r1 : _ Or_error.t)];
      let%bind () =
        match r1 with
        | Ok () -> return ()
        | Error _ ->
          (match%bind Postgres_async.close_finished postgres with
           | Ok _ -> failwith "close_finished returned Ok _ ?"
           | Error _ ->
             printf "close_finished is determined with an error\n";
             (match%bind Postgres_async.query postgres "<dummy>" ~handle_row with
              | Ok _ -> failwith "second query succeeded?"
              | Error _ as r2 ->
                (match show_second_result with
                 | false -> ()
                 | true -> print_s ~hide_positions:true [%message (r2 : _ Or_error.t)]);
                return ()))
      in
      return ()
    in
    with_manual_server ~handle_client ~f:(fun where_to_connect ->
      let%bind outer_result =
        Postgres_async.with_connection
          ~server:where_to_connect
          ~user:"postgres"
          ~database:"postgres"
          ~on_handler_exception:`Raise
          send_queries
      in
      print_s [%message (outer_result : _ Or_error.t)];
      return ())
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
    try_query
      ~show_second_result:true
      (parsecomplete ^ bindcomplete ^ nodata ^ emptyqueryresponse ^ readyforquery)
  in
  [%expect
    {|
    (r1 (Ok _))
    (outer_result (Ok _))
    |}];
  (* unexpected or unrecognised message types at various stages. *)
  let%bind () = try_query "x\x00\x00\x00\x04" in
  [%expect
    {|
    (r1 (
      Error ((query <dummy>) ("Unrecognised message type character" (other x)))))
    close_finished is determined with an error
    (outer_result (Ok _))
    |}];
  let%bind () = try_query (parsecomplete ^ "s\x00\x00\x00\x04") in
  [%expect
    {|
    (r1 (
      Error (
        (query <dummy>)
        ("Unexpected message type"
          (msg_type PortalSuspended)
          (state    Binding)
          (here lib/postgres_async/src/postgres_async.ml:LINE:COL)))))
    close_finished is determined with an error
    (outer_result (Ok _))
    |}];
  let%bind () = try_query (parsecomplete ^ bindcomplete ^ "x\x00\x00\x00\x04") in
  [%expect
    {|
    (r1 (
      Error ((query <dummy>) ("Unrecognised message type character" (other x)))))
    close_finished is determined with an error
    (outer_result (Ok _))
    |}];
  let%bind () =
    try_query (parsecomplete ^ bindcomplete ^ rowdescription ^ "s\x00\x00\x00\x04")
  in
  [%expect
    {|
    (r1 (
      Error (
        (query <dummy>)
        ("Unexpected message type"
          (msg_type PortalSuspended)
          (state    "reading DataRows")
          (here lib/postgres_async/src/postgres_async.ml:LINE:COL)))))
    close_finished is determined with an error
    (outer_result (Ok _))
    |}];
  (* second query fails cleanly too: *)
  let%bind () = try_query "x\x00\x00\x00\x04" ~show_second_result:true in
  [%expect
    {|
    (r1 (
      Error ((query <dummy>) ("Unrecognised message type character" (other x)))))
    close_finished is determined with an error
    (r2 (
      Error (
        (query <dummy>)
        ("query issued against previously-failed connection"
         (original_error ("Unrecognised message type character" (other x)))))))
    (outer_result (Ok _))
    |}];
  (* rowdescription with junk in *)
  let%bind () = try_query (parsecomplete ^ bindcomplete ^ "T\x00\x00\x00\x06\x0f\xff") in
  [%expect
    {|
    (r1 (
      Error (
        (query <dummy>)
        ("Failed to parse RowDescription" (
          exn (Failure "find_null_exn could not find \\x00"))))))
    close_finished is determined with an error
    (outer_result (Ok _))
    |}];
  (* datarow with mismatched number of fields vs. row desc. *)
  let%bind () =
    let z18 = String.init 18 ~f:(fun _ -> '\x00') in
    try_query
      (parsecomplete
       ^ bindcomplete
       ^ "T\x00\x00\x00\x2e\x00\x02A\x00"
       ^ z18
       ^ "B\x00"
       ^ z18
       ^ "D\x00\x00\x00\x0b\x00\x01\x00\x00\x00\x01a"
       ^ commandcomplete
       ^ readyforquery)
  in
  [%expect
    {|
    (r1 (
      Error (
        (query <dummy>)
        ("number of columns in DataRow message did not match RowDescription"
         (column_names (A B))
         (values ((a)))))))
    close_finished is determined with an error
    (outer_result (Ok _))
    |}];
  (* very long datarow *)
  let%bind () =
    try_query
      ~send_eof_after_response:false
      (parsecomplete ^ bindcomplete ^ rowdescription ^ "D\x40\x00\x00\x04 blah blah blah")
  in
  [%expect
    {|
    (r1 (Error ((query <dummy>) ("Message too long" (message_length 1073741829)))))
    close_finished is determined with an error
    (outer_result (Ok _))
    |}];
  (* message truncated by EOF *)
  let%bind () =
    try_query (parsecomplete ^ bindcomplete ^ rowdescription ^ "D\x00\x00\x00\x10")
  in
  [%expect
    {|
    (r1 (Error ((query <dummy>) ("Unexpected EOF" (unconsumed_bytes 5)))))
    close_finished is determined with an error
    (outer_result (Ok _))
    |}];
  return ()
;;

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
      Ivar.fill_exn send_eof_now ();
      (* postgres_async should notice asynchronously, without issuing a query. *)
      let%bind close_finished = close_finished_deferred in
      print_s [%message (close_finished : unit Or_error.t)];
      return ()
    in
    let%bind outer_result =
      Postgres_async.with_connection
        ~server:where_to_connect
        ~user:"postgres"
        ~database:"postgres"
        ~on_handler_exception:`Raise
        handle_connection
    in
    print_s [%message (outer_result : _ Or_error.t)];
    [%expect
      {|
      in connection handler
      (close_finished_deferred Empty)
      now sending EOF
      (close_finished (Error "Unexpected EOF (no unconsumed messages)"))
      (outer_result (Ok _))
      |}];
    return ())
;;

let%expect_test "asynchronous writer failure during login" =
  (* note that SHUTDOWN_RECV doesn't work on TCP; fortunately we have unix sockets. *)
  with_manual_server
    ~handle_client:(fun reader writer ->
      (* wait for the startup message. *)
      let%bind _ = Reader.read_char reader in
      (* shutdown read. *)
      Fd.syscall_exn (Reader.fd reader) (Core_unix.shutdown ~mode:SHUTDOWN_RECEIVE);
      (* ask for a password. *)
      Writer.write writer "R\x00\x00\x00\x0c\x00\x00\x00\x05salt";
      Deferred.never ())
    ~f:(fun where_to_connect ->
      let%bind result =
        Postgres_async.connect
          ()
          ~server:where_to_connect
          ~user:"postgres"
          ~database:"postgres"
          ~password:"postgres"
      in
      let result = Utils.delete_unstable_bits_of_error [%sexp (result : _ Or_error.t)] in
      print_s result;
      [%expect
        {|
        (Error (
          "Writer failed asynchronously" (
            exn (
              monitor.ml.Error
              ("Writer error from inner_monitor"
                (Unix.Unix_error "Broken pipe" writev_assume_fd_is_nonblocking "")
                <omitted>)
              ("Caught by monitor Writer.monitor")))))
        |}];
      return ())
;;

let%expect_test "asynchronous writer failure during query" =
  with_manual_server
    ~handle_client:(fun reader writer ->
      (* login. *)
      let%bind _ = Reader.read_char reader in
      Writer.write writer accept_login;
      (* shutdown read before the query is sent. *)
      Fd.syscall_exn (Reader.fd reader) (Core_unix.shutdown ~mode:SHUTDOWN_RECEIVE);
      Deferred.never ())
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
          ~on_handler_exception:`Raise
          handle_connection
      in
      print_s [%message (outer_result : unit Or_error.t)];
      [%expect
        {|
        connected
        (result (
          Error (
            (query <dummy>)
            ("Writer failed asynchronously" (
              exn (
                monitor.ml.Error
                ("Writer error from inner_monitor"
                  (Unix.Unix_error "Broken pipe" writev_assume_fd_is_nonblocking "")
                  <omitted>)
                ("Caught by monitor Writer.monitor")))))))
        (outer_result (Ok ()))
        |}];
      return ())
;;

(* The two ways the bounded graceful close can time out (see
   [do_close_gracefully_if_possible]): *)

let%expect_test "server that never hangs up after [Terminate]: close is abrupt but \
                 successful"
  =
  with_manual_server
    ~handle_client:(fun reader writer ->
      (* wait for the startup message. *)
      let%bind _ = Reader.read_char reader in
      Writer.write writer accept_login;
      (* Swallow everything else (including [Terminate]) and never close our end; the
         client should give up waiting for our EOF and hang up regardless. *)
      Reader.drain reader)
    ~f:(fun where_to_connect ->
      let%bind postgres =
        Postgres_async.connect
          ()
          ~server:where_to_connect
          ~user:"postgres"
          ~database:"postgres"
        >>| Or_error.ok_exn
      in
      let%bind close_result = Postgres_async.close postgres in
      print_or_error close_result;
      let%bind close_finished = Postgres_async.close_finished postgres in
      print_or_error close_finished;
      [%expect
        {|
        (Ok ())
        (Ok ())
        |}];
      return ())
;;

let%expect_test "close while a query is queued but not yet started: the query is refused \
                 and the close is clean"
  =
  with_manual_server
    ~handle_client:(fun reader writer ->
      (* wait for the startup message. *)
      let%bind _ = Reader.read_char reader in
      Writer.write writer accept_login;
      (* Never respond to the query, and never close our end. *)
      Reader.drain reader)
    ~f:(fun where_to_connect ->
      let%bind postgres =
        Postgres_async.connect
          ()
          ~server:where_to_connect
          ~user:"postgres"
          ~database:"postgres"
        >>| Or_error.ok_exn
      in
      (* [close] is called before the query's turn on the sequencer comes up, so the query
         refuses to run and nothing is written after [Terminate]. *)
      let query_result = Postgres_async.query_expect_no_data postgres "<dummy>" in
      let%bind close_result = Postgres_async.close postgres in
      print_or_error close_result;
      let%bind query_result in
      print_or_error query_result;
      [%expect
        {|
        (Ok ())
        (Error ((query <dummy>) "query issued against connection closed by user"))
        |}];
      return ())
;;

let%expect_test "close while a query is wedged on an unresponsive server: close times \
                 out with an error (and interrupts the query)"
  =
  let query_reached_server = Ivar.create () in
  with_manual_server
    ~handle_client:(fun reader writer ->
      (* consume the whole startup message: an int32 length (which includes itself), then
         the payload. *)
      let read_exactly n =
        let buf = Bytes.create n in
        match%map Reader.really_read reader buf with
        | `Ok -> Bytes.to_string buf
        | `Eof (_ : int) -> raise_s [%message "unexpected EOF in test server"]
      in
      let%bind startup_length = read_exactly 4 in
      let startup_length =
        String.fold startup_length ~init:0 ~f:(fun acc c -> (acc * 256) + Char.to_int c)
      in
      let%bind (_ : string) = read_exactly (startup_length - 4) in
      Writer.write writer accept_login;
      (* wait for the first byte of the query, then go silent: never respond, and never
         close our end. *)
      let%bind (_ : string) = read_exactly 1 in
      Ivar.fill_exn query_reached_server ();
      Reader.drain reader)
    ~f:(fun where_to_connect ->
      let%bind postgres =
        Postgres_async.connect
          ()
          ~server:where_to_connect
          ~user:"postgres"
          ~database:"postgres"
        >>| Or_error.ok_exn
      in
      let query_result = Postgres_async.query_expect_no_data postgres "<dummy>" in
      let%bind () = Ivar.read query_reached_server in
      let%bind close_result = Postgres_async.close postgres in
      print_or_error close_result;
      let%bind query_result in
      print_or_error query_result;
      [%expect
        {|
        (Error
         "close: an in-flight query did not finish within the close grace period, so the connection was torn down forcibly")
        (Error (
          (query <dummy>)
          "close: an in-flight query did not finish within the close grace period, so the connection was torn down forcibly"))
        |}];
      return ())
;;

let%expect_test "close while a simple query is queued behind an in-flight query: the \
                 simple query is refused and nothing is written after [Terminate]"
  =
  (* Unlike the extended-query path, [simple_query]'s sequencer job used to write its
     [Query] message without re-checking [t.state], so a simple query queued behind an
     in-flight query when [close] was called would write behind the already-sent
     [Terminate]. The server checks that [Terminate] is the last thing on the wire. *)
  let parse_complete = "1\x00\x00\x00\x04" in
  let bind_complete = "2\x00\x00\x00\x04" in
  let no_data = "n\x00\x00\x00\x04" in
  let command_complete = "C\x00\x00\x00\x06_\x00" in
  let ready_for_query = "Z\x00\x00\x00\x05I" in
  let close_called = Ivar.create () in
  let server_events = Queue.create () in
  let server_done = Ivar.create () in
  let handle_client reader writer =
    (* consume the whole startup message: an int32 length (which includes itself), then
       the payload. *)
    let read_exactly n =
      let buf = Bytes.create n in
      match%map Reader.really_read reader buf with
      | `Ok -> Bytes.to_string buf
      | `Eof (_ : int) -> raise_s [%message "unexpected EOF in test server"]
    in
    let%bind startup_length = read_exactly 4 in
    let startup_length =
      String.fold startup_length ~init:0 ~f:(fun acc c -> (acc * 256) + Char.to_int c)
    in
    let%bind (_ : string) = read_exactly (startup_length - 4) in
    Writer.write writer accept_login;
    (* Read the in-flight query's messages through [Sync]; it now holds the client-side
       sequencer, with the simple query queued behind it. *)
    let%bind (_ : char list) =
      read_frontend_messages_through reader ~last_message_type:'S' []
    in
    let%bind () = Ivar.read close_called in
    (* Complete the in-flight query. *)
    Writer.write
      writer
      (parse_complete ^ bind_complete ^ no_data ^ command_complete ^ ready_for_query);
    let%bind () = Writer.flushed writer in
    (* [Terminate] was written the moment [close] was called; it should be the next and
       last thing we see. (We never hang up ourselves, so the EOF below only arrives once
       the client's close grace period expires and it tears the socket down; the
       connection was idle by then, so that close still reports success.) *)
    let%bind msg = read_frontend_message reader in
    Queue.enqueue
      server_events
      [%message "message after query completion" ~_:(msg : char)];
    let%bind () =
      match%map Reader.read_char reader with
      | `Eof -> Queue.enqueue server_events [%message "EOF after Terminate"]
      | `Ok c ->
        Queue.enqueue
          server_events
          [%message "data written after Terminate" ~_:(c : char)]
    in
    Ivar.fill_exn server_done ();
    Deferred.never ()
  in
  with_manual_server ~handle_client ~f:(fun where_to_connect ->
    let%bind postgres =
      Postgres_async.connect
        ()
        ~server:where_to_connect
        ~user:"postgres"
        ~database:"postgres"
      >>| Or_error.ok_exn
    in
    let in_flight_query = Postgres_async.query_expect_no_data postgres "<in-flight>" in
    let%bind () = Scheduler.yield_until_no_jobs_remain () in
    let queued_simple_query =
      match%map
        Postgres_async.Private.execute_simple postgres "<queued simple query>"
      with
      | Completed_with_no_warnings (_ : Postgres_async.Command_complete.t list) -> Ok ()
      | Completed_with_warnings ((_ : Postgres_async.Command_complete.t list), warnings)
        -> Or_error.error_s [%message "completed with warnings" (warnings : Error.t list)]
      | Failed e | Connection_error e | Driver_error e ->
        Error (Postgres_async.Private.Pgasync_error.to_error e)
    in
    let%bind () = Scheduler.yield_until_no_jobs_remain () in
    let close_result = Postgres_async.close postgres in
    let%bind () = Scheduler.yield_until_no_jobs_remain () in
    Ivar.fill_exn close_called ();
    let%bind in_flight_result = in_flight_query in
    let%bind queued_simple_result = queued_simple_query in
    let%bind close_result in
    let%bind () = Ivar.read server_done in
    Queue.iter server_events ~f:(fun event ->
      print_s [%message "server" ~_:(event : Sexp.t)]);
    print_s [%message "in-flight query" ~_:(in_flight_result : unit Or_error.t)];
    print_s [%message "queued simple query" ~_:(queued_simple_result : unit Or_error.t)];
    print_s [%message "close" ~_:(close_result : unit Or_error.t)];
    [%expect
      {|
      (server ("message after query completion" X))
      (server "EOF after Terminate")
      ("in-flight query" (Ok ()))
      ("queued simple query" (
        Error (
          (query "<queued simple query>")
          "query issued against connection closed by user")))
      (close (Ok ()))
      |}];
    return ())
;;
