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

let print_simple_query_result ~query_string res =
  match res with
  | Postgres_async.Private.Simple_query_result.Completed_with_no_warnings
      commands_complete ->
    print_s
      [%message
        "Query executed successfully with no warnings"
          (query_string : string)
          (commands_complete : Postgres_async.Command_complete.t list)]
  | Completed_with_warnings (commands_complete, warnings) ->
    print_s
      [%message
        "Query executed successfully with warnings"
          (query_string : string)
          (commands_complete : Postgres_async.Command_complete.t list)
          (warnings : Error.t list)]
  | Connection_error error ->
    print_s [%message "Connection error " (error : Postgres_async.Pgasync_error.t)]
  | Driver_error error ->
    print_s [%message "Driver error " (error : Postgres_async.Pgasync_error.t)]
  | Failed error ->
    print_s [%message "Server reported error " (error : Postgres_async.Pgasync_error.t)]
;;

let simple_query postgres ?(show_column_names = false) query_string =
  let handle_row ~column_names ~values =
    match show_column_names with
    | true ->
      print_s
        [%sexp (Iarray.zip_exn column_names values : (string * string option) iarray)]
    | false -> print_s [%sexp (values : string option iarray)]
  in
  let%bind res = Postgres_async.Private.simple_query postgres ~handle_row query_string in
  print_simple_query_result ~query_string res;
  Deferred.unit
;;

let%expect_test "CREATE + INSERT" =
  with_connection_exn (fun postgres ->
    let query = simple_query postgres in
    let%bind () =
      query "CREATE TEMPORARY TABLE a ( x timestamp, y integer PRIMARY KEY, z text );"
    in
    let%bind () =
      query
        {|
        INSERT INTO a (x, y, z) VALUES
        ('2000-01-01 00:00:00', 1, 'test string'),
        ('2000-01-01 00:00:00', 5, E'nasty\nstring\t''\",x'),
        ('2019-03-14 00:00:00', 10, NULL); |}
    in
    [%expect
      {|
      ("Query executed successfully with no warnings"
       (query_string
        "CREATE TEMPORARY TABLE a ( x timestamp, y integer PRIMARY KEY, z text );")
       (commands_complete (((tag "CREATE TABLE") (rows ())))))
      ("Query executed successfully with no warnings"
       (query_string
         "\
        \n        INSERT INTO a (x, y, z) VALUES\
        \n        ('2000-01-01 00:00:00', 1, 'test string'),\
        \n        ('2000-01-01 00:00:00', 5, E'nasty\\nstring\\t''\\\",x'),\
        \n        ('2019-03-14 00:00:00', 10, NULL); ")
       (commands_complete (((tag INSERT) (rows (3))))))
      |}];
    return ())
;;

let%expect_test "SELECT" =
  with_connection_exn (fun postgres ->
    let query = simple_query postgres in
    let%bind () =
      query "CREATE TEMPORARY TABLE a ( x timestamp, y integer PRIMARY KEY, z text );"
    in
    let%bind () =
      query
        {|
        INSERT INTO a (x, y, z) VALUES
        ('2000-01-01 00:00:00', 1, 'test string'),
        ('2000-01-01 00:00:00', 5, E'nasty\nstring\t''\",x'),
        ('2019-03-14 00:00:00', 10, NULL); |}
    in
    [%expect
      {|
      ("Query executed successfully with no warnings"
       (query_string
        "CREATE TEMPORARY TABLE a ( x timestamp, y integer PRIMARY KEY, z text );")
       (commands_complete (((tag "CREATE TABLE") (rows ())))))
      ("Query executed successfully with no warnings"
       (query_string
         "\
        \n        INSERT INTO a (x, y, z) VALUES\
        \n        ('2000-01-01 00:00:00', 1, 'test string'),\
        \n        ('2000-01-01 00:00:00', 5, E'nasty\\nstring\\t''\\\",x'),\
        \n        ('2019-03-14 00:00:00', 10, NULL); ")
       (commands_complete (((tag INSERT) (rows (3))))))
      |}];
    let%bind () = query "SELECT * FROM a ORDER BY y" in
    [%expect
      {|
      (("2000-01-01 00:00:00") (1) ("test string"))
      (("2000-01-01 00:00:00") (5) ( "nasty\
                                    \nstring\t'\",x"))
      (("2019-03-14 00:00:00") (10) ())
      ("Query executed successfully with no warnings"
       (query_string "SELECT * FROM a ORDER BY y")
       (commands_complete (((tag SELECT) (rows (3))))))
      |}];
    return ())
;;

let%expect_test "Chained CREATE + INSERT + SELECT" =
  with_connection_exn (fun postgres ->
    let query = simple_query postgres in
    let query_string =
      Array.of_list
        [ "CREATE TEMPORARY TABLE a ( x timestamp, y integer PRIMARY KEY, z text );"
        ; {|
        INSERT INTO a (x, y, z) VALUES
        ('2000-01-01 00:00:00', 1, 'test string'),
        ('2000-01-01 00:00:00', 5, E'nasty\nstring\t''\",x'),
        ('2019-03-14 00:00:00', 10, NULL); |}
        ; "SELECT * FROM a ORDER BY y"
        ]
      |> String.concat_array
    in
    let%bind () = query query_string in
    [%expect
      {|
      (("2000-01-01 00:00:00") (1) ("test string"))
      (("2000-01-01 00:00:00") (5) ( "nasty\
                                    \nstring\t'\",x"))
      (("2019-03-14 00:00:00") (10) ())
      ("Query executed successfully with no warnings"
       (query_string
         "CREATE TEMPORARY TABLE a ( x timestamp, y integer PRIMARY KEY, z text );\
        \n        INSERT INTO a (x, y, z) VALUES\
        \n        ('2000-01-01 00:00:00', 1, 'test string'),\
        \n        ('2000-01-01 00:00:00', 5, E'nasty\\nstring\\t''\\\",x'),\
        \n        ('2019-03-14 00:00:00', 10, NULL); SELECT * FROM a ORDER BY y")
       (commands_complete
        (((tag "CREATE TABLE") (rows ())) ((tag INSERT) (rows (3)))
         ((tag SELECT) (rows (3))))))
      |}];
    return ())
;;

let%expect_test "Chained SELECTs" =
  with_connection_exn (fun postgres ->
    let query = simple_query postgres in
    let%bind () =
      query "CREATE TEMPORARY TABLE a ( x timestamp, y integer PRIMARY KEY, z text );"
    in
    let%bind () =
      query
        {|
        INSERT INTO a (x, y, z) VALUES
        ('2000-01-01 00:00:00', 1, 'test string'),
        ('2050-01-01 00:00:00', 5, E'nasty\nstring\t''\",x'),
        ('2019-03-14 00:00:00', 10, NULL); |}
    in
    [%expect
      {|
      ("Query executed successfully with no warnings"
       (query_string
        "CREATE TEMPORARY TABLE a ( x timestamp, y integer PRIMARY KEY, z text );")
       (commands_complete (((tag "CREATE TABLE") (rows ())))))
      ("Query executed successfully with no warnings"
       (query_string
         "\
        \n        INSERT INTO a (x, y, z) VALUES\
        \n        ('2000-01-01 00:00:00', 1, 'test string'),\
        \n        ('2050-01-01 00:00:00', 5, E'nasty\\nstring\\t''\\\",x'),\
        \n        ('2019-03-14 00:00:00', 10, NULL); ")
       (commands_complete (((tag INSERT) (rows (3))))))
      |}];
    let%bind () = query "SELECT x,y FROM a ORDER BY y; SELECT y,z FROM a ORDER BY x" in
    [%expect
      {|
      (("2000-01-01 00:00:00") (1))
      (("2050-01-01 00:00:00") (5))
      (("2019-03-14 00:00:00") (10))
      ((1) ("test string"))
      ((10) ())
      ((5) ( "nasty\
            \nstring\t'\",x"))
      ("Query executed successfully with no warnings"
       (query_string "SELECT x,y FROM a ORDER BY y; SELECT y,z FROM a ORDER BY x")
       (commands_complete (((tag SELECT) (rows (3))) ((tag SELECT) (rows (3))))))
      |}];
    return ())
;;

let%expect_test "Gracefully handles errors without closing connection" =
  with_connection_exn (fun postgres ->
    let query = simple_query postgres in
    let%bind () =
      query "CREATE TEMPORARY TABLE a ( x timestamp, y integer PRIMARY KEY, z text );"
    in
    let%bind () =
      query
        {|
        INSERT INTO a (x, y, z) VALUES
        ('2000-01-01 00:00:00', 1, 'test string'),
        ('2050-01-01 00:00:00', 5, E'nasty\nstring\t''\",x'),
        ('2019-03-14 00:00:00', 10, NULL); |}
    in
    [%expect
      {|
      ("Query executed successfully with no warnings"
       (query_string
        "CREATE TEMPORARY TABLE a ( x timestamp, y integer PRIMARY KEY, z text );")
       (commands_complete (((tag "CREATE TABLE") (rows ())))))
      ("Query executed successfully with no warnings"
       (query_string
         "\
        \n        INSERT INTO a (x, y, z) VALUES\
        \n        ('2000-01-01 00:00:00', 1, 'test string'),\
        \n        ('2050-01-01 00:00:00', 5, E'nasty\\nstring\\t''\\\",x'),\
        \n        ('2019-03-14 00:00:00', 10, NULL); ")
       (commands_complete (((tag INSERT) (rows (3))))))
      |}];
    let%bind () = query "SELECT 1/0; SELECT * FROM a" in
    [%expect
      {|
      ("Server reported error "
       (error
        ((query "SELECT 1/0; SELECT * FROM a")
         ("Postgres Server Error" ((Code 22012) (Message "division by zero"))))))
      |}];
    let%bind () = query "SELECT * FROM a ORDER BY y; SELECT * FROM a ORDER BY x" in
    [%expect
      {|
      (("2000-01-01 00:00:00") (1) ("test string"))
      (("2050-01-01 00:00:00") (5) ( "nasty\
                                    \nstring\t'\",x"))
      (("2019-03-14 00:00:00") (10) ())
      (("2000-01-01 00:00:00") (1) ("test string"))
      (("2019-03-14 00:00:00") (10) ())
      (("2050-01-01 00:00:00") (5) ( "nasty\
                                    \nstring\t'\",x"))
      ("Query executed successfully with no warnings"
       (query_string "SELECT * FROM a ORDER BY y; SELECT * FROM a ORDER BY x")
       (commands_complete (((tag SELECT) (rows (3))) ((tag SELECT) (rows (3))))))
      |}];
    return ())
;;

let%expect_test "Treats chained commands as an implicit transaction" =
  with_connection_exn (fun postgres ->
    let query_exn = simple_query postgres in
    let query = simple_query postgres in
    let%bind () =
      query_exn "CREATE TEMPORARY TABLE a ( x timestamp, y integer PRIMARY KEY, z text );"
    in
    let%bind () =
      query_exn
        {|
        INSERT INTO a (x, y, z) VALUES
        ('2000-01-01 00:00:00', 1, 'test string'),
        ('2050-01-01 00:00:00', 5, E'nasty\nstring\t''\",x'),
        ('2019-03-14 00:00:00', 10, NULL); |}
    in
    [%expect
      {|
      ("Query executed successfully with no warnings"
       (query_string
        "CREATE TEMPORARY TABLE a ( x timestamp, y integer PRIMARY KEY, z text );")
       (commands_complete (((tag "CREATE TABLE") (rows ())))))
      ("Query executed successfully with no warnings"
       (query_string
         "\
        \n        INSERT INTO a (x, y, z) VALUES\
        \n        ('2000-01-01 00:00:00', 1, 'test string'),\
        \n        ('2050-01-01 00:00:00', 5, E'nasty\\nstring\\t''\\\",x'),\
        \n        ('2019-03-14 00:00:00', 10, NULL); ")
       (commands_complete (((tag INSERT) (rows (3))))))
      |}];
    let%bind () = query_exn "SELECT * FROM a ORDER BY y;" in
    [%expect
      {|
      (("2000-01-01 00:00:00") (1) ("test string"))
      (("2050-01-01 00:00:00") (5) ( "nasty\
                                    \nstring\t'\",x"))
      (("2019-03-14 00:00:00") (10) ())
      ("Query executed successfully with no warnings"
       (query_string "SELECT * FROM a ORDER BY y;")
       (commands_complete (((tag SELECT) (rows (3))))))
      |}];
    (* The invalid SELECT command should cause a rollback of the first INSERT *)
    let%bind () =
      query
        {|INSERT INTO a VALUES('1950-01-01 00:00:00',15,NULL);
SELECT 1/0;
INSERT INTO mytable VALUES(2);|}
    in
    [%expect
      {|
      ("Server reported error "
       (error
        ((query
           "INSERT INTO a VALUES('1950-01-01 00:00:00',15,NULL);\
          \nSELECT 1/0;\
          \nINSERT INTO mytable VALUES(2);")
         ("Postgres Server Error" ((Code 22012) (Message "division by zero"))))))
      |}];
    (* We expect the same values to be returned as the first SELECT statement *)
    let%bind () = query_exn "SELECT * FROM a ORDER BY y;" in
    [%expect
      {|
      (("2000-01-01 00:00:00") (1) ("test string"))
      (("2050-01-01 00:00:00") (5) ( "nasty\
                                    \nstring\t'\",x"))
      (("2019-03-14 00:00:00") (10) ())
      ("Query executed successfully with no warnings"
       (query_string "SELECT * FROM a ORDER BY y;")
       (commands_complete (((tag SELECT) (rows (3))))))
      |}];
    return ())
;;

let%expect_test "COPYIN is gracefully handled without terminating connection" =
  with_connection_exn (fun postgres ->
    let query = simple_query postgres in
    let%bind () = query "CREATE TEMPORARY TABLE a (x integer);" in
    let%bind () = query {| INSERT INTO a VALUES (1), (2) |} in
    [%expect
      {|
      ("Query executed successfully with no warnings"
       (query_string "CREATE TEMPORARY TABLE a (x integer);")
       (commands_complete (((tag "CREATE TABLE") (rows ())))))
      ("Query executed successfully with no warnings"
       (query_string " INSERT INTO a VALUES (1), (2) ")
       (commands_complete (((tag INSERT) (rows (2))))))
      |}];
    let%bind () = query "COPY a FROM STDIN; SELECT * FROM a" in
    (* Note that COPY IN fails with an error since we have to send the server a message
       which kills the transfer of information.

       Further statements in the query are not executed.
    *)
    [%expect
      {|
      ("Server reported error "
       (error
        ((query "COPY a FROM STDIN; SELECT * FROM a")
         ("Postgres Server Error"
          ((Code 57014)
           (Message
            "COPY from stdin failed: Command ignored: COPY FROM STDIN is not appropriate for [Postgres_async.simple_query]")
           (Where "COPY a, line 1: \"\""))))))
      |}];
    let%bind () = query "SELECT * FROM a" in
    [%expect
      {|
      ((1))
      ((2))
      ("Query executed successfully with no warnings"
       (query_string "SELECT * FROM a")
       (commands_complete (((tag SELECT) (rows (2))))))
      |}];
    return ())
;;

let%expect_test "COPYIN is gracefully handled in a chained statement" =
  with_connection_exn (fun postgres ->
    let query = simple_query postgres in
    let%bind () = query "CREATE TEMPORARY TABLE a (x integer);" in
    let%bind () = query {| INSERT INTO a VALUES (1), (2) |} in
    [%expect
      {|
      ("Query executed successfully with no warnings"
       (query_string "CREATE TEMPORARY TABLE a (x integer);")
       (commands_complete (((tag "CREATE TABLE") (rows ())))))
      ("Query executed successfully with no warnings"
       (query_string " INSERT INTO a VALUES (1), (2) ")
       (commands_complete (((tag INSERT) (rows (2))))))
      |}];
    (* Should only return an error message since the error at the first statement prevents
       the SELECT from running *)
    let%bind () = query "COPY a FROM STDIN; SELECT * FROM a" in
    [%expect
      {|
      ("Server reported error "
       (error
        ((query "COPY a FROM STDIN; SELECT * FROM a")
         ("Postgres Server Error"
          ((Code 57014)
           (Message
            "COPY from stdin failed: Command ignored: COPY FROM STDIN is not appropriate for [Postgres_async.simple_query]")
           (Where "COPY a, line 1: \"\""))))))
      |}];
    (* Should return both the results of the SELECT and an error message *)
    let%bind () = query "SELECT * FROM a; COPY a FROM STDIN;" in
    [%expect
      {|
      ((1))
      ((2))
      ("Server reported error "
       (error
        ((query "SELECT * FROM a; COPY a FROM STDIN;")
         ("Postgres Server Error"
          ((Code 57014)
           (Message
            "COPY from stdin failed: Command ignored: COPY FROM STDIN is not appropriate for [Postgres_async.simple_query]")
           (Where "COPY a, line 1: \"\""))))))
      |}];
    let%bind () = query "SELECT * FROM a" in
    [%expect
      {|
      ((1))
      ((2))
      ("Query executed successfully with no warnings"
       (query_string "SELECT * FROM a")
       (commands_complete (((tag SELECT) (rows (2))))))
      |}];
    return ())
;;

let%expect_test "COPYOUT is gracefully handled without terminating connection" =
  with_connection_exn (fun postgres ->
    let query = simple_query postgres in
    let%bind () = query "CREATE TEMPORARY TABLE a (x integer);" in
    let%bind () = query {| INSERT INTO a VALUES (1), (2) |} in
    [%expect
      {|
      ("Query executed successfully with no warnings"
       (query_string "CREATE TEMPORARY TABLE a (x integer);")
       (commands_complete (((tag "CREATE TABLE") (rows ())))))
      ("Query executed successfully with no warnings"
       (query_string " INSERT INTO a VALUES (1), (2) ")
       (commands_complete (((tag INSERT) (rows (2))))))
      |}];
    let%bind () = query "COPY a TO STDOUT" in
    [%expect
      {|
      ("Query executed successfully with warnings"
       (query_string "COPY a TO STDOUT")
       (commands_complete (((tag COPY) (rows (2)))))
       (warnings
        ("Command ignored: COPY TO STDOUT is not appropriate for [Postgres_async.simple_query]")))
      |}];
    let%bind () = query "SELECT * FROM a" in
    [%expect
      {|
      ((1))
      ((2))
      ("Query executed successfully with no warnings"
       (query_string "SELECT * FROM a")
       (commands_complete (((tag SELECT) (rows (2))))))
      |}];
    return ())
;;

let%expect_test "COPY OUT is gracefully handled in a chained statement" =
  with_connection_exn (fun postgres ->
    let query = simple_query postgres in
    let%bind () = query "CREATE TEMPORARY TABLE a (x integer);" in
    let%bind () = query {| INSERT INTO a VALUES (1), (2) |} in
    [%expect
      {|
      ("Query executed successfully with no warnings"
       (query_string "CREATE TEMPORARY TABLE a (x integer);")
       (commands_complete (((tag "CREATE TABLE") (rows ())))))
      ("Query executed successfully with no warnings"
       (query_string " INSERT INTO a VALUES (1), (2) ")
       (commands_complete (((tag INSERT) (rows (2))))))
      |}];
    (* Should still run the full statement since the client-side prevents COPY OUT, not
       the server *)
    let%bind () = query "COPY a TO STDOUT; SELECT * FROM a" in
    [%expect
      {|
      ((1))
      ((2))
      ("Query executed successfully with warnings"
       (query_string "COPY a TO STDOUT; SELECT * FROM a")
       (commands_complete (((tag COPY) (rows (2))) ((tag SELECT) (rows (2)))))
       (warnings
        ("Command ignored: COPY TO STDOUT is not appropriate for [Postgres_async.simple_query]")))
      |}];
    let%bind () = query "SELECT * FROM a; COPY a TO STDOUT;" in
    [%expect
      {|
      ((1))
      ((2))
      ("Query executed successfully with warnings"
       (query_string "SELECT * FROM a; COPY a TO STDOUT;")
       (commands_complete (((tag SELECT) (rows (2))) ((tag COPY) (rows (2)))))
       (warnings
        ("Command ignored: COPY TO STDOUT is not appropriate for [Postgres_async.simple_query]")))
      |}];
    let%bind () = query "SELECT * FROM a" in
    [%expect
      {|
      ((1))
      ((2))
      ("Query executed successfully with no warnings"
       (query_string "SELECT * FROM a")
       (commands_complete (((tag SELECT) (rows (2))))))
      |}];
    return ())
;;

let%expect_test "Chained SELECTs work across multiple tables" =
  with_connection_exn (fun postgres ->
    let _query = simple_query postgres in
    let query = simple_query postgres in
    let%bind () =
      query
        "CREATE TEMPORARY TABLE a (x integer); CREATE TEMPORARY TABLE b (y text); CREATE \
         TEMPORARY TABLE c (z boolean)"
    in
    let%bind () =
      query
        "INSERT INTO a VALUES (1), (2); INSERT INTO b VALUES ('foo'), ('bar');\n\
        \         INSERT INTO c VALUES (true), (false);"
    in
    [%expect
      {|
      ("Query executed successfully with no warnings"
       (query_string
        "CREATE TEMPORARY TABLE a (x integer); CREATE TEMPORARY TABLE b (y text); CREATE TEMPORARY TABLE c (z boolean)")
       (commands_complete
        (((tag "CREATE TABLE") (rows ())) ((tag "CREATE TABLE") (rows ()))
         ((tag "CREATE TABLE") (rows ())))))
      ("Query executed successfully with no warnings"
       (query_string
         "INSERT INTO a VALUES (1), (2); INSERT INTO b VALUES ('foo'), ('bar');\
        \n         INSERT INTO c VALUES (true), (false);")
       (commands_complete
        (((tag INSERT) (rows (2))) ((tag INSERT) (rows (2)))
         ((tag INSERT) (rows (2))))))
      |}];
    let%bind () = query "SELECT * FROM a; SELECT * FROM b order by y; SELECT * FROM C;" in
    [%expect
      {|
      ((1))
      ((2))
      ((bar))
      ((foo))
      ((t))
      ((f))
      ("Query executed successfully with no warnings"
       (query_string
        "SELECT * FROM a; SELECT * FROM b order by y; SELECT * FROM C;")
       (commands_complete
        (((tag SELECT) (rows (2))) ((tag SELECT) (rows (2)))
         ((tag SELECT) (rows (2))))))
      |}];
    return ())
;;

let%expect_test "COPY OUT generates warnings but doesn't prevent query execution" =
  with_connection_exn (fun postgres ->
    let query = simple_query postgres in
    let%bind () =
      query
        "CREATE TEMPORARY TABLE a (x integer); CREATE TEMPORARY TABLE b (y text); CREATE \
         TEMPORARY TABLE c (z boolean)"
    in
    let%bind () =
      query
        "INSERT INTO a VALUES (1), (2); INSERT INTO b VALUES ('foo'), ('bar');\n\
        \         INSERT INTO c VALUES (true), (false); COPY a TO STDOUT; INSERT INTO a \
         VALUES (3)"
    in
    [%expect
      {|
      ("Query executed successfully with no warnings"
       (query_string
        "CREATE TEMPORARY TABLE a (x integer); CREATE TEMPORARY TABLE b (y text); CREATE TEMPORARY TABLE c (z boolean)")
       (commands_complete
        (((tag "CREATE TABLE") (rows ())) ((tag "CREATE TABLE") (rows ()))
         ((tag "CREATE TABLE") (rows ())))))
      ("Query executed successfully with warnings"
       (query_string
         "INSERT INTO a VALUES (1), (2); INSERT INTO b VALUES ('foo'), ('bar');\
        \n         INSERT INTO c VALUES (true), (false); COPY a TO STDOUT; INSERT INTO a VALUES (3)")
       (commands_complete
        (((tag INSERT) (rows (2))) ((tag INSERT) (rows (2)))
         ((tag INSERT) (rows (2))) ((tag COPY) (rows (2)))
         ((tag INSERT) (rows (1)))))
       (warnings
        ("Command ignored: COPY TO STDOUT is not appropriate for [Postgres_async.simple_query]")))
      |}];
    let%bind () = query "SELECT * FROM a" in
    [%expect
      {|
      ((1))
      ((2))
      ((3))
      ("Query executed successfully with no warnings"
       (query_string "SELECT * FROM a")
       (commands_complete (((tag SELECT) (rows (3))))))
      |}];
    return ())
;;

let%expect_test "Exception handling in handle_row" =
  with_connection_exn (fun postgres ->
    let handle_row ~column_names:_ ~values:_ =
      raise_s [%message "raising in handle_row"]
    in
    match%bind
      Deferred.Or_error.try_with (fun () ->
        Postgres_async.Private.simple_query postgres ~handle_row "select * from pg_class")
    with
    | Ok _ -> raise_s [%message "unexpected ok"]
    | Error (_ : Error.t) ->
      print_endline "Got exn from simple_query";
      Postgres_async.status postgres |> [%sexp_of: Postgres_async.state] |> print_s;
      [%expect
        {|
        Got exn from simple_query
        Open
        |}];
      return ())
;;

let%expect_test "Exception handling in handle_column" =
  with_connection_exn (fun postgres ->
    let handle_row ~column_names:_ ~values:_ = () in
    let handle_columns _ = raise_s [%message "raising in handle column"] in
    match%bind
      Deferred.Or_error.try_with (fun () ->
        Postgres_async.Private.simple_query
          ~handle_columns
          postgres
          ~handle_row
          "select * from pg_class")
    with
    | Ok _ -> raise_s [%message "unexpected ok"]
    | Error (_ : Error.t) ->
      print_endline "Got exn from simple_query";
      Postgres_async.status postgres |> [%sexp_of: Postgres_async.state] |> print_s;
      [%expect
        {|
        Got exn from simple_query
        Open
        |}];
      return ())
;;

let%expect_test "handle_column" =
  with_connection_exn (fun postgres ->
    let query query_string =
      let column_data = ref None in
      let handle_row ~column_names ~values =
        let column_data =
          Option.map !column_data ~f:(fun arr ->
            Iarray.map arr ~f:(fun data ->
              Postgres_async.Column_metadata.(name data, pg_type_oid data)))
        in
        print_s
          [%message
            (values : string option iarray)
              (column_names : string iarray)
              (column_data : (string * int) iarray option)]
      in
      let handle_columns arr =
        print_endline "Handle columns called";
        column_data := Some arr
      in
      Postgres_async.Private.simple_query
        postgres
        ~handle_columns
        ~handle_row
        query_string
      >>| Postgres_async.Private.Simple_query_result.to_or_pgasync_error
      >>| Postgres_async.Or_pgasync_error.to_or_error
      >>| Or_error.ignore_m
      >>| Or_error.ok_exn
    in
    let%bind () =
      query "CREATE TEMPORARY TABLE a ( x timestamp, y integer PRIMARY KEY, z text );"
    in
    let%bind () =
      query
        {|
        INSERT INTO a (x, y, z) VALUES
        ('2000-01-01 00:00:00', 1, 'test string'),
        ('2000-01-01 00:00:00', 5, E'nasty\nstring\t''\",x'),
        ('2019-03-14 00:00:00', 10, NULL); |}
    in
    [%expect {| |}];
    let%bind () =
      query
        "SELECT * FROM a ORDER BY y;BEGIN;SELECT x,y from a order by y ;COMMIT; SELECT z \
         from a order by z"
    in
    [%expect
      {|
      Handle columns called
      ((values (("2000-01-01 00:00:00") (1) ("test string")))
       (column_names (x y z)) (column_data (((x 1114) (y 23) (z 25)))))
      ((values (("2000-01-01 00:00:00") (5) ( "nasty\
                                             \nstring\t'\",x")))
       (column_names (x y z)) (column_data (((x 1114) (y 23) (z 25)))))
      ((values (("2019-03-14 00:00:00") (10) ())) (column_names (x y z))
       (column_data (((x 1114) (y 23) (z 25)))))
      Handle columns called
      ((values (("2000-01-01 00:00:00") (1))) (column_names (x y))
       (column_data (((x 1114) (y 23)))))
      ((values (("2000-01-01 00:00:00") (5))) (column_names (x y))
       (column_data (((x 1114) (y 23)))))
      ((values (("2019-03-14 00:00:00") (10))) (column_names (x y))
       (column_data (((x 1114) (y 23)))))
      Handle columns called
      ((values (( "nasty\
                 \nstring\t'\",x")))
       (column_names (z)) (column_data (((z 25)))))
      ((values (("test string"))) (column_names (z)) (column_data (((z 25)))))
      ((values (())) (column_names (z)) (column_data (((z 25)))))
      |}];
    return ())
;;

let%expect_test "execute_simple" =
  with_connection_exn (fun postgres ->
    let exec query_string =
      Postgres_async.Private.execute_simple postgres query_string
      >>| print_simple_query_result ~query_string
    in
    let%bind () = exec "CREATE TEMPORARY TABLE t (x int, y int)" in
    [%expect
      {|
      ("Query executed successfully with no warnings"
       (query_string "CREATE TEMPORARY TABLE t (x int, y int)")
       (commands_complete (((tag "CREATE TABLE") (rows ())))))
      |}];
    let%bind () = exec "ALTER TABLE t ADD COLUMN z int" in
    [%expect
      {|
      ("Query executed successfully with no warnings"
       (query_string "ALTER TABLE t ADD COLUMN z int")
       (commands_complete (((tag "ALTER TABLE") (rows ())))))
      |}];
    let%bind () = exec "INSERT INTO t (x, y, z) VALUES (1,2,3)" in
    [%expect
      {|
      ("Query executed successfully with no warnings"
       (query_string "INSERT INTO t (x, y, z) VALUES (1,2,3)")
       (commands_complete (((tag INSERT) (rows (1))))))
      |}];
    (* Multiple queries, all of which return no rows *)
    let%bind () =
      exec
        {| INSERT INTO t (x, y, z) VALUES (10,20,30);
           CREATE TEMPORARY TABLE u (a int);
           ALTER TABLE u ADD COLUMN b INT; |}
    in
    [%expect
      {|
      ("Query executed successfully with no warnings"
       (query_string
         " INSERT INTO t (x, y, z) VALUES (10,20,30);\
        \n           CREATE TEMPORARY TABLE u (a int);\
        \n           ALTER TABLE u ADD COLUMN b INT; ")
       (commands_complete
        (((tag INSERT) (rows (1))) ((tag "CREATE TABLE") (rows ()))
         ((tag "ALTER TABLE") (rows ())))))
      |}];
    (* Run a query that returns rows, check that it fails *)
    let%bind () = exec "SELECT x,y,z FROM t" in
    [%expect
      {|
      ("Driver error "
       (error "[Postgres_async.execute_simple]: query returned at least one row"))
      |}];
    (* A query that returns no rows, followed by a query that returns rows, followed by a
       query that doesn't return rows *)
    let%bind () =
      exec
        {| INSERT INTO t (x, y, z) VALUES (10,20,30);
           SELECT * FROM t;
           CREATE TABLE v (x int); |}
    in
    [%expect
      {|
      ("Driver error "
       (error "[Postgres_async.execute_simple]: query returned at least one row"))
      |}];
    (* Check that the third query above (the one that *didn't* return rows) still
       succeeded *)
    let%bind () = exec "DROP TABLE v" in
    [%expect
      {|
      ("Query executed successfully with no warnings" (query_string "DROP TABLE v")
       (commands_complete (((tag "DROP TABLE") (rows ())))))
      |}];
    return ())
;;
