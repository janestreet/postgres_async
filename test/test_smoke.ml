open! Core
open Async

let harness = lazy (Harness.create ())

let%expect_test "check that basic query functionality works" =
  Harness.with_connection_exn (force harness) ~database:"postgres" (fun postgres ->
    let%bind result =
      Postgres_async.query
        postgres
        "SELECT $1::int"
        ~parameters:[| Some "1234" |]
        ~handle_row:(fun ~column_names:_ ~values ->
          print_s [%message (values : string option array)])
    in
    Or_error.ok_exn result;
    [%expect {| (values ((1234))) |}];
    return ())
;;

let%expect_test "check that fundamental copy-in features work" =
  Harness.with_connection_exn (force harness) ~database:"postgres" (fun postgres ->
    let%bind result =
      Postgres_async.query_expect_no_data
        postgres
        "CREATE TEMPORARY TABLE x ( y integer PRIMARY KEY, z text )"
    in
    Or_error.ok_exn result;
    [%expect {| |}];
    let%bind result =
      let countdown = ref 10 in
      Postgres_async.copy_in_rows
        postgres
        ~table_name:"x"
        ~column_names:[ "y"; "z" ]
        ~feed_data:(fun () ->
          match !countdown with
          | 0 -> Finished
          | i ->
            decr countdown;
            Data
              [| Some (Int.to_string i)
               ; Option.some_if (i % 2 = 0) (sprintf "asdf-%i" i)
              |])
    in
    Or_error.ok_exn result;
    [%expect {| |}];
    let%bind result =
      Postgres_async.query
        postgres
        "SELECT * FROM x ORDER BY y"
        ~handle_row:(fun ~column_names ~values ->
          print_s
            [%sexp (Array.zip_exn column_names values : (string * string option) array)])
    in
    Or_error.ok_exn result;
    [%expect
      {|
      ((y (1)) (z ()))
      ((y (2)) (z (asdf-2)))
      ((y (3)) (z ()))
      ((y (4)) (z (asdf-4)))
      ((y (5)) (z ()))
      ((y (6)) (z (asdf-6)))
      ((y (7)) (z ()))
      ((y (8)) (z (asdf-8)))
      ((y (9)) (z ()))
      ((y (10)) (z (asdf-10)))
      |}];
    return ())
;;
