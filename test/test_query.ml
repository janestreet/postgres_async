open Core
open Async

let with_connection_exn =
  let database = "test_query" in
  let harness =
    lazy (
      let h = Harness.create () in
      Harness.create_database h database;
      h)
  in
  fun func ->
    Harness.with_connection_exn (force harness) ~database func

let query_exn postgres ?(show_column_names=false) ?parameters ?pushback str =
  let handle_row ~column_names ~values =
    match show_column_names with
    | true ->
      print_s [%sexp (Array.zip_exn column_names values : (string * string option) array)]
    | false ->
      print_s [%sexp (values : string option array)]
  in
  let%bind res = Postgres_async.query postgres ?parameters ?pushback str ~handle_row in
  Or_error.ok_exn res;
  return ()

let%expect_test "column names and ordering" =
  with_connection_exn (fun postgres ->
    let query_exn = query_exn postgres ~show_column_names:true in
    let%bind () =
      query_exn "CREATE TEMPORARY TABLE a ( x timestamp, y integer PRIMARY KEY, z text );"
    in
    let%bind () =
      query_exn {|
        INSERT INTO a (x, y, z) VALUES
        ('2000-01-01 00:00:00', 1, 'test string'),
        ('2000-01-01 00:00:00', 5, E'nasty\nstring\t''\",x'),
        ('2019-03-14 00:00:00', 10, NULL); |}
    in
    let%bind () = query_exn "SELECT * FROM a ORDER BY y" in
    let%bind () =
      [%expect {|
        ((x ("2000-01-01 00:00:00")) (y (1)) (z ("test string")))
        ((x ("2000-01-01 00:00:00")) (y (5)) (z ( "nasty\
                                                 \nstring\t'\",x")))
        ((x ("2019-03-14 00:00:00")) (y (10)) (z ())) |}]
    in
    let%bind () = query_exn "SELECT z, x FROM a ORDER BY y" in
    let%bind () =
      [%expect {|
        ((z ("test string")) (x ("2000-01-01 00:00:00")))
        ((z ( "nasty\
             \nstring\t'\",x")) (x ("2000-01-01 00:00:00")))
        ((z ()) (x ("2019-03-14 00:00:00"))) |}]
    in
    let%bind () = query_exn "SELECT x as moo, y FROM a ORDER BY y" in
    let%bind () =
      [%expect {|
        ((moo ("2000-01-01 00:00:00")) (y (1)))
        ((moo ("2000-01-01 00:00:00")) (y (5)))
        ((moo ("2019-03-14 00:00:00")) (y (10))) |}]
    in
    return ()
  )

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
    let%bind () =
      [%expect {|
        ((1) (-5))
        ((-5) ())
        ((1000000) (1000000)) |}]
    in
    let%bind () =
      query_exn "SELECT $1::text" ~parameters:[|Some "nasty\nstring\t''\",x"|]
    in
    let%bind () =
      [%expect {|
        (( "nasty\
          \nstring\t''\",x")) |}]
    in
    let%bind () =
      query_exn "CREATE TEMPORARY TABLE c ( x integer, y text )"
    in
    let%bind () =
      (* and now with more type inference: *)
      query_exn
        "INSERT INTO c (x, y) VALUES ($1, $2), ($3, $4)"
        ~parameters:[| Some "5"; Some "five"; Some "10"; None |]
    in
    let%bind () = query_exn "SELECT * FROM c" in
    let%bind () =
      [%expect {|
        ((5) (five))
        ((10) ()) |}]
    in
    let%bind () =
      query_exn
        "UPDATE c SET y = 'ten' WHERE x = $1 RETURNING x, y"
        ~parameters:[| Some "10" |]
    in
    let%bind () =
      [%expect {|
        ((10) (ten)) |}]
    in
    return ()
  )

let%expect_test "queries where Describe Portal = NoData are accepted by [query]" =
  (* i.e., it's not necessary to use [query_expect_no_data] *)
  with_connection_exn (fun postgres ->
    let%bind () =
      query_exn
        postgres
        "CREATE TEMPORARY TABLE d ( x integer )"
    in
    return ()
  )

let%expect_test "empty query" =
  with_connection_exn (fun postgres ->
    Deferred.List.iter [""; "-- comment"] ~f:(fun str -> query_exn postgres str)
  )

let%expect_test "failures are reported gracefully and don't kill the connection" =
  with_connection_exn (fun postgres ->
    let query ?parameters str =
      match%bind
        Postgres_async.query
          ?parameters
          postgres
          str
          ~handle_row:(fun ~column_names:_ ~values ->
            print_s [%message "row" ~_:(values : string option array)]
          )
      with
      | Ok () ->
        print_s [%message "OK"];
        return ()
      | Error err ->
        let err = Utils.delete_unstable_bit_of_server_error (Error.sexp_of_t err) in
        print_s [%message "Error" ~_:(err : Sexp.t)];
        return ()
    in
    let%bind () = query "syntactically invalid" in
    let%bind () =
      [%expect {|
        (Error
         ("Postgres Server Error" (state Parsing) ((severity ERROR) (code 42601)))) |}]
    in
    (* but we can still use the connection just fine *)
    let%bind () = query "SELECT 1" in
    let%bind () =
      [%expect {|
        (row ((1)))
        OK |}]
    in
    (* let's try errors at other stages. *)
    let%bind () = query "SELECT $1::int" ~parameters:[|Some "a"|] in
    let%bind () =
      [%expect {|
        (Error
         ("Postgres Server Error" (state Binding) ((severity ERROR) (code 22P02)))) |}]
    in
    let%bind () = query "DO $$ BEGIN RAISE 'hi'; END $$" in
    let%bind () =
      [%expect {|
        (Error
         ("Postgres Server Error" (state Executing) ((severity ERROR) (code P0001)))) |}]
    in
    let%bind () = query "SELECT 'everything is fine'" in
    let%bind () =
      [%expect {|
        (row (("everything is fine")))
        OK |}]
    in
    (* let's try executing a sql statement "that is not a query" *)
    let%bind () = query "CREATE TEMPORARY TABLE c ( x integer )" in
    let%bind () = [%expect {| OK |}] in
    let%bind () = query "COPY c FROM STDIN" in
    let%bind () =
      [%expect {|
        (Error "COPY FROM STDIN is not appropriate for [Postgres_async.query]") |}]
    in
    let%bind () = query "COPY c TO STDOUT" in
    let%bind () =
      [%expect {|
        (Error "COPY TO STDOUT is not appropriate for [Postgres_async.query]") |}]
    in
    (* note that our COPY c from STDIN would have otherwise worked: *)
    let%bind result =
      let once = ref false in
      Postgres_async.copy_in_raw
        postgres
        "COPY c FROM STDIN"
        ~feed_data:(fun () ->
          match !once with
          | true -> Finished
          | false ->
            once := true;
            Data "10\n"
        )
    in
    Or_error.ok_exn result;
    (* and the connection is certainly still fine. *)
    let%bind () = query "SELECT * FROM c" in
    let%bind () =
      [%expect {|
        (row ((10)))
        OK |}]
    in
    (* note that in the case of a side-effecting copy-out, it is allowed to run to
       completion despite us returning the error (the alternative is killing the
       connection, or preemptively wrapping everything in begin/commit, which would be
       annoying to implement and not worth it) *)
    let%bind () = query "COPY (INSERT INTO c (x) VALUES (20) RETURNING x) TO STDOUT" in
    let%bind () =
      [%expect {|
        (Error "COPY TO STDOUT is not appropriate for [Postgres_async.query]") |}]
    in
    (* Observe that the values were in fact inserted: *)
    let%bind () = query "SELECT * FROM c ORDER BY x" in
    let%bind () =
      [%expect {|
        (row ((10)))
        (row ((20)))
        OK |}]
    in
    return ()
  )

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
    let%bind () = [%expect {| ((1)) |}] in
    let%bind () = query_exn postgres "SELECT COUNT(*) FROM x" in
    let%bind () = [%expect {| ((0)) |}] in
    (* but if we do have a transaction... *)
    let%bind () = query_exn postgres "BEGIN" in
    let%bind () = query_exn postgres "INSERT INTO x VALUES (1) RETURNING y" in
    let%bind () = [%expect {| ((1)) |}] in
    let%bind () = query_exn postgres "SELECT COUNT(*) FROM x" in
    let%bind () = [%expect {| ((1)) |}] in
    let%bind () = query_exn postgres "COMMIT" in
    let%bind () = query_exn postgres "SELECT COUNT(*) FROM x" in
    let%bind () = [%expect {| ((0)) |}] in
    return ()
  )

let%expect_test "transactions" =
  with_connection_exn (fun postgres ->
    (* This test is kinda just testing the postgres server, since nothing in the client
       library really worries about transactions at all; we're just sending statements to
       the server. However, this is important enough that it seems prudent to check that
       it works, lest we introduce some silly bug that breaks this... somehow... *)
    let%bind () =
      query_exn postgres "CREATE TABLE transaction_test ( y integer )"
    in
    (* since each statement runs in one transaction, inserts are immediately visible to
       other connections *)
    let%bind () = query_exn postgres "INSERT INTO transaction_test VALUES (1)" in
    let%bind () = [%expect {| |}] in
    let%bind () =
      with_connection_exn (fun postgres ->
        let%bind () = query_exn postgres "SELECT COUNT(*) FROM transaction_test" in
        let%bind () = [%expect {| ((1)) |}] in
        return ()
      )
    in
    (* but if we're in a transaction, that's not true. *)
    let%bind () = query_exn postgres "BEGIN" in
    let%bind () = query_exn postgres "INSERT INTO transaction_test VALUES (1)" in
    let%bind () = [%expect {| |}] in
    let%bind () =
      with_connection_exn (fun inner_postgres ->
        let%bind () = query_exn inner_postgres "SELECT COUNT(*) FROM transaction_test" in
        let%bind () = [%expect {| ((1)) |}] in
        let%bind () = query_exn postgres "COMMIT" (* outer postgres! *) in
        let%bind () = query_exn inner_postgres "SELECT COUNT(*) FROM transaction_test" in
        let%bind () = [%expect {| ((2)) |}] in
        return ()
      )
    in
    (* test rollback *)
    let%bind () = query_exn postgres "BEGIN" in
    let%bind () = query_exn postgres "INSERT INTO transaction_test VALUES (1)" in
    let%bind () = query_exn postgres "SELECT COUNT(*) FROM transaction_test" in
    let%bind () = [%expect {| ((3)) |}] in
    let%bind () = query_exn postgres "ROLLBACK" in
    let%bind () = query_exn postgres "SELECT COUNT(*) FROM transaction_test" in
    let%bind () = [%expect {| ((2)) |}] in
    return ()
  )

let%expect_test "pushback" =
  with_connection_exn (fun postgres ->
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
    let one_kb = String.init 1024 ~f:(const 'a') in
    let%bind () =
      query_exn postgres "UPDATE x SET z = $1" ~parameters:[|Some one_kb|]
    in
    let rows_handled = ref 0 in
    let reached_10000_rows = Ivar.create () in
    let ok_to_proceed_past_10000_rows = Ivar.create () in
    let calls_to_pushback = ref 0 in
    let query_done_deferred =
      Postgres_async.query
        postgres
        "SELECT y, z FROM x ORDER BY y"
        ~handle_row:(fun ~column_names:_ ~values ->
          let (y, z) =
            match values with
            | [|Some y; Some z|] -> (y, z)
            | _ -> assert false
          in
          assert (String.equal z one_kb);
          assert (Int.equal (Int.of_string y) !rows_handled);
          incr rows_handled
        )
        ~pushback:(fun () ->
          incr calls_to_pushback;
          match !rows_handled > 10000 with
          | false -> return ()
          | true ->
            Ivar.fill_if_empty reached_10000_rows ();
            Ivar.read ok_to_proceed_past_10000_rows
        )
    in
    (* We'll stop inside the first call to pushback where rows_handled > 10000 *)
    let%bind () = Ivar.read reached_10000_rows in
    (* Rows should not be delivered while we're pushing back. *)
    let rows_handled_before_epoll = !rows_handled in
    assert (rows_handled_before_epoll < 20000);
    let%bind () = (force Utils.do_an_epoll) () in
    [%test_eq: int] rows_handled_before_epoll !rows_handled;
    (* Unblock, and the query will complete. *)
    Ivar.fill ok_to_proceed_past_10000_rows ();
    let%bind result = query_done_deferred in
    Or_error.ok_exn result;
    print_s [%message "query complete" (rows_handled : int ref)];
    let%bind () = [%expect {| ("query complete" (rows_handled 65536)) |}] in
    (* There will have been many many calls to pushback, since the result is big. *)
    assert (!calls_to_pushback > 10);
    return ()
  )

let%expect_test "query expect no data" =
  with_connection_exn (fun postgres ->
    let query_expect_no_data ?parameters str =
      match%bind Postgres_async.query_expect_no_data postgres ?parameters str with
      | Ok () ->
        print_s [%message "OK"];
        return ()
      | Error err ->
        let err = Utils.delete_unstable_bit_of_server_error (Error.sexp_of_t err) in
        print_s [%message "Error" ~_:(err : Sexp.t)];
        return ()
    in
    (* It's fine for a query to be one that has a rowdescription, as long as it doesn't
       produce any output. *)
    let%bind () = query_expect_no_data "SELECT 1 WHERE FALSE" in
    let%bind () = [%expect {| OK |}] in
    let%bind () = query_expect_no_data "CREATE TEMPORARY TABLE c ( x integer )" in
    let%bind () = [%expect {| OK |}] in
    let%bind () = query_expect_no_data "SELECT * FROM c" in
    let%bind () = [%expect {| OK |}] in
    let%bind () = query_expect_no_data "INSERT INTO c (x) VALUES (10)" in
    let%bind () = [%expect {| OK |}] in
    (* These are not fine. *)
    let%bind () = query_expect_no_data "SELECT 1" in
    let%bind () = [%expect {| (Error "query unexpectedly produced rows") |}] in
    let%bind () = query_expect_no_data "COPY c FROM STDIN" in
    let%bind () =
      [%expect {|
        (Error "[Postgres_async.query_expect_no_data]: query attempted COPY IN") |}]
    in
    let%bind () = query_expect_no_data "COPY c TO STDOUT" in
    let%bind () =
      [%expect {|
        (Error "[Postgres_async.query_expect_no_data]: query attempted COPY OUT") |}]
    in
    (* Note that queries with side effects are currently allowed to run to completion
       despite an error being returned. See the comment by the similar tests for [query]
       for details. *)
    let%bind () = query_expect_no_data "INSERT INTO c (x) VALUES (20) RETURNING x" in
    let%bind () =
      [%expect {|
        (Error "query unexpectedly produced rows") |}]
    in
    let%bind () =
      query_expect_no_data "COPY (INSERT INTO c (x) VALUES (20) RETURNING x) TO STDOUT"
    in
    let%bind () =
      [%expect {|
        (Error "[Postgres_async.query_expect_no_data]: query attempted COPY OUT") |}]
    in
    (* Observe that the values were in fact inserted: *)
    let%bind () = query_exn postgres "SELECT * FROM c ORDER BY x" in
    let%bind () =
      [%expect {|
        ((10))
        ((20))
        ((20)) |}]
    in
    return ()
  )
