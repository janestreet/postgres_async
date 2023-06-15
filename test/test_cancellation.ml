open! Core
open Async

let () = Backtrace.elide := true
let harness = lazy (Harness.create ())

let%expect_test "Demonstrate that cancelling running queries is possible" =
  let harness = force harness in
  let%bind connection =
    Postgres_async.connect
      ()
      ~server:(Harness.where_to_connect harness)
      ~user:"postgres"
      ~database:"postgres"
  in
  let connection = Or_error.ok_exn connection in
  let query_to_cancel =
    Postgres_async.query
      connection
      "select 1 from pg_sleep(100)"
      ~handle_row:(fun ~column_names ~values ->
        raise_s
          [%message
            "Unexpectedly produced rows"
              (column_names : string array)
              (values : string option array)])
  in
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  let%bind cancel_result = Postgres_async.Private.pq_cancel connection in
  let%bind query_to_cancel = Clock_ns.with_timeout Time_ns.Span.second query_to_cancel in
  let%bind begin_result = Postgres_async.query_expect_no_data connection "BEGIN" in
  let%bind close_result = Postgres_async.close connection in
  print_s
    [%message
      (query_to_cancel : unit Or_error.t Clock_ns.Or_timeout.t)
        (cancel_result : unit Or_error.t)
        (begin_result : unit Or_error.t)
        (close_result : unit Or_error.t)];
  [%expect
    {|
    ((query_to_cancel
      (Result
       (Error
        ("Error during query execution (despite parsing ok)"
         ((Code 57014) (Message "canceling statement due to user request"))))))
     (cancel_result (Ok ())) (begin_result (Ok ())) (close_result (Ok ()))) |}];
  [%expect {||}];
  return ()
;;
