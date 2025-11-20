open Core
open Async

let with_connection_exn =
  let database = "test_query" in
  let harness =
    lazy
      (let h = Harness.create () in
       Harness.create_database h database;
       h)
  in
  fun func -> Harness.with_connection_exn (force harness) ~database func
;;

let print_cut_after_first ppf r =
  if not !r then Format.pp_print_space ppf ();
  r := false
;;

let row_handler ~show_column_names =
  stage (fun (local_ row_handle) ->
    Format.printf "@[<h>(";
    let first = ref true in
    Postgres_async.Row_handle.foldi row_handle ~init:() ~f:(fun ~column ~value () ->
      let value =
        match value with
        | None -> None
        | Some iobuf -> Some (Iobuf.Peek.To_string.subo iobuf)
      in
      let sexp =
        if show_column_names
        then
          [%sexp
            (Postgres_async.Column_metadata.name column : string), (value : string option)]
        else [%sexp (value : string option)]
      in
      Format.printf "%a%a" print_cut_after_first first Sexp.pp_hum sexp);
    Format.printf ")@]@;")
;;

let query_exn
  postgres
  ?zero_copy
  ?handle_columns
  ?(show_column_names = false)
  ?parameters
  ?pushback
  str
  =
  let run ~zero_copy =
    match zero_copy with
    | false ->
      let handle_row ~column_names ~values =
        match show_column_names with
        | true ->
          print_s
            [%sexp (Iarray.zip_exn column_names values : (string * string option) iarray)]
        | false -> print_s [%sexp (values : string option iarray)]
      in
      Postgres_async.query postgres ?handle_columns ?parameters ?pushback str ~handle_row
      >>| ok_exn
    | true ->
      Postgres_async.query_zero_copy
        postgres
        ?handle_columns
        ?parameters
        ?pushback
        str
        ~handle_row:(row_handler ~show_column_names |> unstage)
      >>| ok_exn
  in
  match zero_copy with
  | Some zero_copy -> run ~zero_copy
  | None ->
    (match
       let str = String.strip str in
       List.exists ~f:(fun prefix -> String.is_prefix str ~prefix) [ "SELECT"; "WITH" ]
     with
     | false ->
       (* For non-read-only commands, choose at random which implementation to use. *)
       run ~zero_copy:(Random.bool ())
     | true ->
       (* For read-only commands, run twice as an easy way to confirm these interfaces are
          equivalent. *)
       let%bind () = run ~zero_copy:false in
       let output1 = Expect_test_helpers_core.expect_test_output () in
       let%bind () = run ~zero_copy:true in
       let output2 = Expect_test_helpers_core.expect_test_output () in
       [%test_result: Sexp.t list]
         ~expect:(Sexp.of_string_many output1)
         (Sexp.of_string_many output2);
       print_endline output2;
       return ())
;;

let%expect_test "column names and ordering" =
  with_connection_exn (fun postgres ->
    let query_exn = query_exn postgres ~show_column_names:true in
    let%bind () =
      query_exn "CREATE TEMPORARY TABLE a ( x timestamp, y integer PRIMARY KEY, z text );"
    in
    let%bind () =
      query_exn
        {|
        INSERT INTO a (x, y, z) VALUES
        ('2000-01-01 00:00:00', 1, 'test string'),
        ('2000-01-01 00:00:00', 5, E'nasty\nstring\t''\",x'),
        ('2019-03-14 00:00:00', 10, NULL); |}
    in
    let%bind () = query_exn "SELECT * FROM a ORDER BY y" in
    [%expect
      {|
      ((x ("2000-01-01 00:00:00")) (y (1)) (z ("test string")))
      ((x ("2000-01-01 00:00:00")) (y (5)) (z ( "nasty\
                                               \nstring\t'\",x")))
      ((x ("2019-03-14 00:00:00")) (y (10)) (z ()))
      |}];
    let%bind () = query_exn "SELECT z, x FROM a ORDER BY y" in
    [%expect
      {|
      ((z ("test string")) (x ("2000-01-01 00:00:00")))
      ((z ( "nasty\
           \nstring\t'\",x")) (x ("2000-01-01 00:00:00")))
      ((z ()) (x ("2019-03-14 00:00:00")))
      |}];
    let%bind () = query_exn "SELECT x as moo, y FROM a ORDER BY y" in
    [%expect
      {|
      ((moo ("2000-01-01 00:00:00")) (y (1)))
      ((moo ("2000-01-01 00:00:00")) (y (5)))
      ((moo ("2019-03-14 00:00:00")) (y (10)))
      |}];
    return ())
;;

let%expect_test "reading raw iobufs" =
  with_connection_exn (fun postgres ->
    let query_exn = query_exn postgres ~zero_copy:true ~show_column_names:true in
    let%bind () =
      query_exn "CREATE TEMPORARY TABLE a ( x timestamp, y integer PRIMARY KEY, z text );"
    in
    let%bind () =
      query_exn
        {|
        INSERT INTO a (x, y, z) VALUES
        ('2000-01-01 00:00:00', 1, 'test string'),
        ('2000-01-01 00:00:00', 5, E'nasty\nstring\t''\",x'),
        ('2019-03-14 00:00:00', 10, NULL); |}
    in
    let%bind () = query_exn "SELECT * FROM a ORDER BY y" in
    [%expect
      {|
      ((x ("2000-01-01 00:00:00")) (y (1)) (z ("test string")))
      ((x ("2000-01-01 00:00:00")) (y (5)) (z ( "nasty\
                                               \nstring\t'\",x")))
      ((x ("2019-03-14 00:00:00")) (y (10)) (z ()))
      |}];
    let%bind () = query_exn "SELECT z, x FROM a ORDER BY y" in
    [%expect
      {|
      ((z ("test string")) (x ("2000-01-01 00:00:00")))
      ((z ( "nasty\
           \nstring\t'\",x")) (x ("2000-01-01 00:00:00")))
      ((z ()) (x ("2019-03-14 00:00:00")))
      |}];
    let%bind () = query_exn "SELECT x as moo, y FROM a ORDER BY y" in
    [%expect
      {|
      ((moo ("2000-01-01 00:00:00")) (y (1)))
      ((moo ("2000-01-01 00:00:00")) (y (5)))
      ((moo ("2019-03-14 00:00:00")) (y (10)))
      |}];
    return ())
;;

let%expect_test "parameters" =
  with_connection_exn (fun postgres ->
    let query_exn = query_exn postgres in
    let%bind () =
      query_exn
        {|
          WITH b AS (
                  SELECT 1 as s , $1::int as x , $2::int as y
            UNION SELECT 2      , $2           , $3
            UNION SELECT 3      , $4           , $4)
          SELECT x, y FROM b ORDER BY s
        |}
        ~parameters:[| Some "1"; Some "-5"; None; Some "1000000" |]
    in
    [%expect {| ((1) (-5)) ((-5) ()) ((1000000) (1000000)) |}];
    let%bind () =
      query_exn "SELECT $1::text" ~parameters:[| Some "nasty\nstring\t''\",x" |]
    in
    [%expect
      {|
      (( "nasty\
        \nstring\t''\",x"))
      |}];
    let%bind () = query_exn "CREATE TEMPORARY TABLE c ( x integer, y text )" in
    let%bind () =
      (* and now with more type inference: *)
      query_exn
        "INSERT INTO c (x, y) VALUES ($1, $2), ($3, $4)"
        ~parameters:[| Some "5"; Some "five"; Some "10"; None |]
    in
    let%bind () = query_exn "SELECT * FROM c" in
    [%expect {| ((5) (five)) ((10) ()) |}];
    let%bind () =
      query_exn
        "UPDATE c SET y = 'ten' WHERE x = $1 RETURNING x, y"
        ~parameters:[| Some "10" |]
    in
    [%expect {| ((10) (ten)) |}];
    return ())
;;

let%expect_test "queries where Describe Portal = NoData are accepted by [query]" =
  (* i.e., it's not necessary to use [query_expect_no_data] *)
  with_connection_exn (fun postgres ->
    let%bind () = query_exn postgres "CREATE TEMPORARY TABLE d ( x integer )" in
    return ())
;;

let%expect_test "empty query" =
  with_connection_exn (fun postgres ->
    Deferred.List.iter ~how:`Sequential [ ""; "-- comment" ] ~f:(fun str ->
      query_exn postgres str))
;;

let%expect_test "failures are reported gracefully and don't kill the connection" =
  with_connection_exn (fun postgres ->
    let query ?parameters str =
      match%bind
        Postgres_async.query
          ?parameters
          postgres
          str
          ~handle_row:(fun ~column_names:_ ~values ->
            print_s [%message "row" ~_:(values : string option iarray)])
      with
      | Ok () ->
        print_s [%message "OK"];
        return ()
      | Error err ->
        let err = Utils.delete_unstable_bits_of_error (Error.sexp_of_t err) in
        print_s [%message "Error" ~_:(err : Sexp.t)];
        return ()
    in
    let%bind () = query "syntactically invalid" in
    [%expect
      {|
      (Error
       ((query "syntactically invalid")
        ("Postgres Server Error (state=Parsing)" ((Code 42601)))))
      |}];
    (* but we can still use the connection just fine *)
    let%bind () = query "SELECT 1" in
    [%expect
      {|
      (row ((1)))
      OK
      |}];
    (* let's try errors at other stages. *)
    let%bind () = query "SELECT $1::int" ~parameters:[| Some "a" |] in
    [%expect
      {|
      (Error
       (((query "SELECT $1::int") (parameters ((a))))
        ("Postgres Server Error (state=Binding)" ((Code 22P02)))))
      |}];
    let%bind () = query "DO $$ BEGIN RAISE 'hi'; END $$" in
    [%expect
      {|
      (Error
       ((query "DO $$ BEGIN RAISE 'hi'; END $$")
        ("Postgres Server Error (state=Executing)" ((Code P0001)))))
      |}];
    let%bind () = query "SELECT 'everything is fine'" in
    [%expect
      {|
      (row (("everything is fine")))
      OK
      |}];
    (* let's try executing a sql statement "that is not a query" *)
    let%bind () = query "CREATE TEMPORARY TABLE c ( x integer )" in
    [%expect {| OK |}];
    let%bind () = query "COPY c FROM STDIN" in
    [%expect
      {|
      (Error
       ((query "COPY c FROM STDIN")
        "COPY FROM STDIN is not appropriate for [Postgres_async.query]"))
      |}];
    let%bind () = query "COPY c TO STDOUT" in
    [%expect
      {|
      (Error
       ((query "COPY c TO STDOUT")
        "COPY TO STDOUT is not appropriate for [Postgres_async.query]"))
      |}];
    (* note that our COPY c from STDIN would have otherwise worked: *)
    let%bind result =
      let once = ref false in
      Postgres_async.copy_in_raw postgres "COPY c FROM STDIN" ~feed_data:(fun () ->
        match !once with
        | true -> Finished
        | false ->
          once := true;
          Data "10\n")
    in
    Or_error.ok_exn result;
    (* and the connection is certainly still fine. *)
    let%bind () = query "SELECT * FROM c" in
    [%expect
      {|
      (row ((10)))
      OK
      |}];
    (* note that in the case of a side-effecting copy-out, it is allowed to run to
       completion despite us returning the error (the alternative is killing the
       connection, or preemptively wrapping everything in begin/commit, which would be
       annoying to implement and not worth it) *)
    let%bind () = query "COPY (INSERT INTO c (x) VALUES (20) RETURNING x) TO STDOUT" in
    [%expect
      {|
      (Error
       ((query "COPY (INSERT INTO c (x) VALUES (20) RETURNING x) TO STDOUT")
        "COPY TO STDOUT is not appropriate for [Postgres_async.query]"))
      |}];
    (* Observe that the values were in fact inserted: *)
    let%bind () = query "SELECT * FROM c ORDER BY x" in
    [%expect
      {|
      (row ((10)))
      (row ((20)))
      OK
      |}];
    return ())
;;

let%expect_test "by default each statement runs in its own transaction" =
  with_connection_exn (fun postgres ->
    (* we'll use a temporary table with "on commit delete rows" as a canary for whether or
       not we're in a transaction *)
    let%bind () =
      query_exn postgres "CREATE TEMPORARY TABLE x ( y integer ) ON COMMIT DELETE ROWS"
    in
    (* the default is (just like in the protocol) that each statement runs in its own
       transaction, and so no rows survive to the SELECT *)
    let%bind () = query_exn postgres "INSERT INTO x VALUES (1) RETURNING y" in
    [%expect {| ((1)) |}];
    let%bind () = query_exn postgres "SELECT COUNT(*) FROM x" in
    [%expect {| ((0)) |}];
    (* but if we do have a transaction... *)
    let%bind () = query_exn postgres "BEGIN" in
    let%bind () = query_exn postgres "INSERT INTO x VALUES (1) RETURNING y" in
    [%expect {| ((1)) |}];
    let%bind () = query_exn postgres "SELECT COUNT(*) FROM x" in
    [%expect {| ((1)) |}];
    let%bind () = query_exn postgres "COMMIT" in
    let%bind () = query_exn postgres "SELECT COUNT(*) FROM x" in
    [%expect {| ((0)) |}];
    return ())
;;

let%expect_test "transactions" =
  with_connection_exn (fun postgres ->
    (* This test is kinda just testing the postgres server, since nothing in the client
       library really worries about transactions at all; we're just sending statements to
       the server. However, this is important enough that it seems prudent to check that
       it works, lest we introduce some silly bug that breaks this... somehow... *)
    let%bind () = query_exn postgres "CREATE TABLE transaction_test ( y integer )" in
    (* since each statement runs in one transaction, inserts are immediately visible to
       other connections *)
    let%bind () = query_exn postgres "INSERT INTO transaction_test VALUES (1)" in
    [%expect {| |}];
    let%bind () =
      with_connection_exn (fun postgres ->
        let%bind () = query_exn postgres "SELECT COUNT(*) FROM transaction_test" in
        [%expect {| ((1)) |}];
        return ())
    in
    (* but if we're in a transaction, that's not true. *)
    let%bind () = query_exn postgres "BEGIN" in
    let%bind () = query_exn postgres "INSERT INTO transaction_test VALUES (1)" in
    [%expect {| |}];
    let%bind () =
      with_connection_exn (fun inner_postgres ->
        let%bind () = query_exn inner_postgres "SELECT COUNT(*) FROM transaction_test" in
        [%expect {| ((1)) |}];
        let%bind () = query_exn postgres "COMMIT" (* outer postgres! *) in
        let%bind () = query_exn inner_postgres "SELECT COUNT(*) FROM transaction_test" in
        [%expect {| ((2)) |}];
        return ())
    in
    (* test rollback *)
    let%bind () = query_exn postgres "BEGIN" in
    let%bind () = query_exn postgres "INSERT INTO transaction_test VALUES (1)" in
    let%bind () = query_exn postgres "SELECT COUNT(*) FROM transaction_test" in
    [%expect {| ((3)) |}];
    let%bind () = query_exn postgres "ROLLBACK" in
    let%bind () = query_exn postgres "SELECT COUNT(*) FROM transaction_test" in
    [%expect {| ((2)) |}];
    return ())
;;

let one_kb_of_a = lazy (String.init 1024 ~f:(const 'a'))

let make_very_large_table_x postgres =
  (* make a table with 65k rows. *)
  let%bind () =
    query_exn
      postgres
      {|
        DO $$ BEGIN
          CREATE TEMPORARY TABLE x ( y integer, z text );
          INSERT INTO x (y) VALUES (0);
          FOR i IN 0..15 LOOP
            INSERT INTO x (y) SELECT power(2, i) + y FROM x;
          END LOOP;
        END $$
      |}
  in
  (* now make the rows big (so, ~65Mb) *)
  let%bind () =
    query_exn postgres "UPDATE x SET z = $1" ~parameters:[| Some (force one_kb_of_a) |]
  in
  return ()
;;

let%expect_test "pushback" =
  with_connection_exn (fun postgres ->
    let%bind () = make_very_large_table_x postgres in
    let rows_handled = ref 0 in
    let reached_10000_rows = Ivar.create () in
    let ok_to_proceed_past_10000_rows = Ivar.create () in
    let calls_to_pushback = ref 0 in
    let query_done_deferred =
      Postgres_async.query
        postgres
        "SELECT y, z FROM x ORDER BY y"
        ~handle_row:(fun ~column_names:_ ~values ->
          let y, z =
            match Iarray.to_array values with
            | [| Some y; Some z |] -> y, z
            | _ -> assert false
          in
          assert (String.equal z (force one_kb_of_a));
          assert (Int.equal (Int.of_string y) !rows_handled);
          incr rows_handled)
        ~pushback:(fun () ->
          incr calls_to_pushback;
          match !rows_handled > 10000 with
          | false -> return ()
          | true ->
            Ivar.fill_if_empty reached_10000_rows ();
            Ivar.read ok_to_proceed_past_10000_rows)
    in
    (* We'll stop inside the first call to pushback where rows_handled > 10000 *)
    let%bind () = Ivar.read reached_10000_rows in
    (* Rows should not be delivered while we're pushing back. *)
    let rows_handled_before_epoll = !rows_handled in
    assert (rows_handled_before_epoll < 20000);
    let%bind () = (force Utils.do_an_epoll) () in
    [%test_eq: int] rows_handled_before_epoll !rows_handled;
    (* Unblock, and the query will complete. *)
    Ivar.fill_exn ok_to_proceed_past_10000_rows ();
    let%bind result = query_done_deferred in
    Or_error.ok_exn result;
    print_s [%message "query complete" (rows_handled : int ref)];
    [%expect {| ("query complete" (rows_handled 65536)) |}];
    (* There will have been many many calls to pushback, since the result is big. *)
    assert (!calls_to_pushback > 10);
    return ())
;;

let%expect_test "query expect no data" =
  with_connection_exn (fun postgres ->
    let query_expect_no_data ?parameters str =
      match%bind Postgres_async.query_expect_no_data postgres ?parameters str with
      | Ok () ->
        print_s [%message "OK"];
        return ()
      | Error err ->
        let err = Utils.delete_unstable_bits_of_error (Error.sexp_of_t err) in
        print_s [%message "Error" ~_:(err : Sexp.t)];
        return ()
    in
    (* It's fine for a query to be one that has a rowdescription, as long as it doesn't
       produce any output. *)
    let%bind () = query_expect_no_data "SELECT 1 WHERE FALSE" in
    [%expect {| OK |}];
    let%bind () = query_expect_no_data "CREATE TEMPORARY TABLE c ( x integer )" in
    [%expect {| OK |}];
    let%bind () = query_expect_no_data "SELECT * FROM c" in
    [%expect {| OK |}];
    let%bind () = query_expect_no_data "INSERT INTO c (x) VALUES (10)" in
    [%expect {| OK |}];
    (* These are not fine. *)
    let%bind () = query_expect_no_data "SELECT 1" in
    [%expect {| (Error ((query "SELECT 1") "query unexpectedly produced rows")) |}];
    let%bind () = query_expect_no_data "COPY c FROM STDIN" in
    [%expect
      {|
      (Error
       ((query "COPY c FROM STDIN")
        "[Postgres_async.query_expect_no_data]: query attempted COPY IN"))
      |}];
    let%bind () = query_expect_no_data "COPY c TO STDOUT" in
    [%expect
      {|
      (Error
       ((query "COPY c TO STDOUT")
        "[Postgres_async.query_expect_no_data]: query attempted COPY OUT"))
      |}];
    (* Note that queries with side effects are currently allowed to run to completion
       despite an error being returned. See the comment by the similar tests for [query]
       for details. *)
    let%bind () = query_expect_no_data "INSERT INTO c (x) VALUES (20) RETURNING x" in
    [%expect
      {|
      (Error
       ((query "INSERT INTO c (x) VALUES (20) RETURNING x")
        "query unexpectedly produced rows"))
      |}];
    let%bind () =
      query_expect_no_data "COPY (INSERT INTO c (x) VALUES (20) RETURNING x) TO STDOUT"
    in
    [%expect
      {|
      (Error
       ((query "COPY (INSERT INTO c (x) VALUES (20) RETURNING x) TO STDOUT")
        "[Postgres_async.query_expect_no_data]: query attempted COPY OUT"))
      |}];
    (* Observe that the values were in fact inserted: *)
    let%bind () = query_exn postgres "SELECT * FROM c ORDER BY x" in
    [%expect {| ((10)) ((20)) ((20)) |}];
    return ())
;;

let%expect_test "insane number of parameters" =
  (* Committing this offence actually kills the connection because we fail to write the
     'bind' message and the code as currently structured won't recover from that.

     This is a reasonable stance to take since in general if we fail to write a message
     we're arguably desynchronised and should bail out. Admittedly though, for this
     specific case we _could_ recover. We just don't care to. *)
  with_connection_exn (fun postgres ->
    match%bind
      Postgres_async.query_expect_no_data
        postgres
        ""
        ~parameters:(Array.init 100_000 ~f:(fun _ -> None))
    with
    | Ok () -> assert false
    | Error err ->
      let err = Utils.delete_unstable_bits_of_error (Error.sexp_of_t err) in
      print_s [%message "Error" ~_:(err : Sexp.t)];
      [%expect
        {|
        (Error
         (((query "")
           (parameters
            (() () () () () () () () () () () () () () ()
             ("remaining 99984 parameter(s) omitted"))))
          ("Writer.write_gen_whole: error writing value"
           (exn (Failure "uint16 out of range: 100000")))))
        |}];
      return ())
;;

let%expect_test "query terminated mid execution" =
  with_connection_exn (fun postgres ->
    let%bind backend_pid = Utils.pg_backend_pid postgres in
    let%bind () = make_very_large_table_x postgres in
    let rows_handled = ref 0 in
    let reached_10000_rows = Ivar.create () in
    let ok_to_proceed_past_10000_rows = Ivar.create () in
    let query_done_deferred =
      Postgres_async.query
        postgres
        "SELECT y, z FROM x ORDER BY y"
        ~handle_row:(fun ~column_names:_ ~values:_ -> incr rows_handled)
        ~pushback:(fun () ->
          match !rows_handled > 10000 with
          | false -> return ()
          | true ->
            Ivar.fill_if_empty reached_10000_rows ();
            Ivar.read ok_to_proceed_past_10000_rows)
    in
    let%bind () = Ivar.read reached_10000_rows in
    let%bind () =
      with_connection_exn (fun postgres2 ->
        query_exn
          postgres2
          "SELECT pg_cancel_backend($1)"
          ~parameters:[| Some backend_pid |])
    in
    [%expect {| ((t)) |}];
    Ivar.fill_exn ok_to_proceed_past_10000_rows ();
    let%bind () =
      match%bind query_done_deferred with
      | Ok () -> assert false
      | Error err ->
        let err = Utils.delete_unstable_bits_of_error (Error.sexp_of_t err) in
        print_s [%message "Error" ~_:(err : Sexp.t)];
        return ()
    in
    [%expect
      {|
      (Error
       ((query "SELECT y, z FROM x ORDER BY y")
        ("Error during query execution (despite parsing ok)" ((Code 57014)))))
      |}];
    assert (!rows_handled < 50000);
    (* note that the connection remains healthy. *)
    let%bind () = query_exn postgres "SELECT 1" in
    [%expect {| ((1)) |}];
    return ())
;;

let%expect_test "the handle_column callback" =
  with_connection_exn (fun postgres ->
    (* Here are what the type OIDs mentioned below correspond to: *)
    let%bind () =
      query_exn
        postgres
        "SELECT oid, typname from pg_type where oid in (1114, 23, 25)"
        ~show_column_names:true
    in
    [%expect
      {|
      ((oid (23)) (typname (int4))) ((oid (25)) (typname (text)))
      ((oid (1114)) (typname (timestamp)))
      |}];
    let query_exn =
      query_exn
        postgres
        ~handle_columns:(fun desc ->
          let columns =
            Iarray.map desc ~f:(fun column ->
              let name = Postgres_async.Column_metadata.name column in
              let pg_type_oid = Postgres_async.Column_metadata.pg_type_oid column in
              name, pg_type_oid)
          in
          print_s [%message (columns : (string * int) Iarray.t)])
        ~show_column_names:true
    in
    (* Setup two tables and a view. *)
    let%bind () =
      query_exn "CREATE TEMPORARY TABLE a ( x timestamp, y integer PRIMARY KEY, z text );"
    in
    let%bind () =
      query_exn "CREATE TEMPORARY TABLE b ( x timestamp, y integer PRIMARY KEY, z text );"
    in
    let%bind () = query_exn "CREATE TEMPORARY VIEW vw_a AS select * from a" in
    let%bind () =
      query_exn
        {|
        INSERT INTO a (x, y, z) VALUES
        ('2000-01-01 00:00:00', 1, 'test string'),
        ('2019-03-14 00:00:00', 10, NULL); |}
    in
    (* [handle_columns] is called once, before any rows are returned. *)
    let%bind () = query_exn "SELECT * FROM a ORDER BY y" in
    [%expect
      {|
      (columns ((x 1114) (y 23) (z 25)))
      ((x ("2000-01-01 00:00:00")) (y (1)) (z ("test string")))
      ((x ("2019-03-14 00:00:00")) (y (10)) (z ()))
      |}];
    (* Column names and types follow ordering of SQL Select *)
    let%bind () = query_exn "SELECT z, x FROM a ORDER BY y" in
    [%expect
      {|
      (columns ((z 25) (x 1114)))
      ((z ("test string")) (x ("2000-01-01 00:00:00")))
      ((z ()) (x ("2019-03-14 00:00:00")))
      |}];
    (* Renames captured in column names *)
    let%bind () = query_exn "SELECT x as moo, y FROM a ORDER BY y" in
    [%expect
      {|
      (columns ((moo 1114) (y 23)))
      ((moo ("2000-01-01 00:00:00")) (y (1)))
      ((moo ("2019-03-14 00:00:00")) (y (10)))
      |}];
    (* Column names and types are provided even when no rows are returned *)
    let%bind () = query_exn "SELECT * FROM a LIMIT 0" in
    [%expect {| (columns ((x 1114) (y 23) (z 25))) |}];
    (* Column names and types are provided for views *)
    let%bind () = query_exn "SELECT * FROM vw_a LIMIT 0" in
    [%expect {| (columns ((x 1114) (y 23) (z 25))) |}];
    (* Column names and types are provided on joins *)
    let%bind () =
      query_exn "SELECT vw_a.x, vw_a.y, b.y, b.z FROM vw_a join b on vw_a.y = b.y"
    in
    [%expect {| (columns ((x 1114) (y 23) (y 23) (z 25))) |}];
    return ())
;;

let%expect_test "handle_column raising prevents any call to handle_row" =
  with_connection_exn (fun postgres ->
    let%bind result =
      Monitor.try_with (fun () ->
        query_exn
          postgres
          ~handle_columns:(fun (_ : Postgres_async.Column_metadata.t iarray) ->
            raise_s [%message "Intentionally raising in handle_columns"])
          ~show_column_names:true
          "SELECT * from pg_type")
    in
    match result with
    | Ok () -> raise_s [%message "Unexpected success"]
    | Error exn ->
      print_s [%message "Got expected exn" (exn : exn)];
      [%expect
        {|
        ("Got expected exn"
         (exn
          (monitor.ml.Error "Intentionally raising in handle_columns"
           ("<backtrace elided in test>"))))
        |}];
      return ())
;;
