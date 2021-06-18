open Core
open Async
open Expect_test_helpers_core

let with_connection =
  let database = "test_error_code" in
  let harness =
    lazy
      (let h = Harness.create () in
       Harness.create_database h database;
       h)
  in
  fun func ->
    Postgres_async.Expert.with_connection
      ~user:"postgres"
      ~server:(Harness.where_to_connect (force harness))
      ~database
      ~on_handler_exception:`Raise
      func
;;

let print_or_pgasync_error or_pgasync_error =
  match or_pgasync_error with
  | Ok () -> print_s [%sexp Ok ()]
  | Error error ->
    let error_code = Postgres_async.Pgasync_error.postgres_error_code error in
    let severity = Postgres_async.Pgasync_error.postgres_field error Severity in
    let as_error =
      Postgres_async.Pgasync_error.to_error error
      |> [%sexp_of: Error.t]
      |> Utils.delete_unstable_bits_of_error
    in
    print_s
      [%message
        "Error"
          (error_code : string option)
          (severity : string option)
          (as_error : Sexp.t)]
;;

let%expect_test "deadlock_detected has error_code=40P01" =
  let%bind connection_result =
    with_connection (fun postgres ->
      let%bind result =
        Postgres_async.Expert.query_expect_no_data
          postgres
          "DO $$ BEGIN RAISE deadlock_detected; END; $$"
      in
      print_or_pgasync_error result;
      [%expect
        {|
        (Error
          (error_code (40P01))
          (severity   (ERROR))
          (as_error ("Postgres Server Error (state=Executing)" ((Code 40P01))))) |}];
      return ())
  in
  print_or_pgasync_error connection_result;
  [%expect {| (Ok ()) |}];
  return ()
;;

let%expect_test "error_code is erased from the result of query against a dead connection" =
  (* note, by the way, that in this test we're mising [Postgres_async.foo] functions (via
     [Utils.pg_backend_pid] and [Postgres_async.Expert.foo] functions, as one is allowed
     to do. *)
  let%bind result =
    with_connection (fun postgres ->
      let%bind backend_pid = Utils.pg_backend_pid postgres in
      let%bind result =
        with_connection (fun postgres2 ->
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
      Postgres_async.Or_pgasync_error.ok_exn result;
      (* The close-finished error does have the error code, since it was this error code
         that caused the problem. *)
      let%bind result = Postgres_async.Expert.close_finished postgres in
      print_or_pgasync_error result;
      [%expect
        {|
        (Error
          (error_code (57P01))
          (severity   (FATAL))
          (as_error (
            "ErrorResponse received asynchronously, assuming connection is dead"
            ((Severity FATAL)
             (Code     57P01))))) |}];
      (* Attempting to issue new queries against the connection produces an error that
         specifies what the original error was, but does not claim the error code, since
         this error is not directly attributable to this query and it would be misleading
         to claim that it was the error code of this query (see comment in
         postgres_async.ml). *)
      let%bind result = Postgres_async.Expert.query_expect_no_data postgres "" in
      print_or_pgasync_error result;
      [%expect
        {|
        (Error
          (error_code ())
          (severity   ())
          (as_error (
            "query issued against previously-failed connection"
            (original_error (
              "ErrorResponse received asynchronously, assuming connection is dead"
              ((Severity FATAL)
               (Code     57P01))))))) |}];
      return ())
  in
  print_or_pgasync_error result;
  [%expect {| (Ok ()) |}];
  return ()
;;
