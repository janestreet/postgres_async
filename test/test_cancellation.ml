open! Core
open Async

let () = Backtrace.elide := true
let harness = lazy (Harness.create ())

let%expect_test ("Demonstrate that cancelling running queries is possible" [@tags
                                                                             "disabled"])
  =
  let harness = force harness in
  let user = "postgres" in
  let connect () =
    Postgres_async.connect
      ()
      ~server:(Harness.where_to_connect harness)
      ~user
      ~database:"postgres"
    >>| Or_error.ok_exn
  in
  let%bind connection = connect () in
  let%bind activity_connection = connect () in
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
  let%bind () =
    Deferred.repeat_until_finished () (fun () ->
      let count = ref 0 in
      let%map () =
        Postgres_async.query
          activity_connection
          [%string
            "select * from pg_stat_activity where state = 'active' and query like \
             '%pg_sleep%'"]
          ~handle_row:(fun ~column_names:_ ~values:_ -> incr count)
        >>| Or_error.ok_exn
      in
      match Int.equal !count 2 with
      | true -> `Finished ()
      | false -> `Repeat ())
  in
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

let%expect_test "Parse full backend key range" =
  let secret_sent = Int32.max_value |> Int32.to_int_exn in
  let pid_sent = 9999 in
  print_s [%message (pid_sent : int) (secret_sent : int)];
  let buf = Iobuf.create ~len:8 in
  Iobuf.Fill.int32_be_trunc buf pid_sent;
  Iobuf.Fill.int32_be_trunc buf secret_sent;
  Iobuf.flip_lo buf;
  let { Postgres_async__.Types.pid; secret } =
    Postgres_async.Private.Protocol.Backend.BackendKeyData.consume buf |> Or_error.ok_exn
  in
  print_s [%message (pid : int) (secret : int)];
  [%expect
    {|
    ((pid_sent 9999) (secret_sent 2147483647))
    ((pid 9999) (secret 2147483647)) |}];
  Deferred.unit
;;
