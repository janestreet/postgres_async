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
          (as_error (
            (query "DO $$ BEGIN RAISE deadlock_detected; END; $$")
            ("Postgres Server Error (state=Executing)" ((Code 40P01))))))
        |}];
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
             (Code     57P01)))))
        |}];
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
          (error_code (08003))
          (severity ())
          (as_error (
            (query "")
            ("query issued against previously-failed connection"
             (original_error (
               "ErrorResponse received asynchronously, assuming connection is dead"
               ((Severity FATAL)
                (Code     57P01))))))))
        |}];
      return ())
  in
  print_or_pgasync_error result;
  [%expect {| (Ok ()) |}];
  return ()
;;

let%expect_test "reporting syntax errors for short queries includes full query" =
  let%bind connection_result =
    with_connection (fun postgres ->
      let%bind result =
        Postgres_async.Expert.query_expect_no_data postgres "select foo, bar from baz"
      in
      print_or_pgasync_error result;
      [%expect
        {|
        (Error
          (error_code (42P01))
          (severity   (ERROR))
          (as_error (
            (query "select foo, bar from baz")
            ("Postgres Server Error (state=Parsing)" ((Code 42P01))))))
        |}];
      return ())
  in
  print_or_pgasync_error connection_result;
  [%expect {| (Ok ()) |}];
  return ()
;;

let%expect_test "reporting syntax errors for long queries shrinks context in a \
                 meaningful way"
  =
  let%bind connection_result =
    with_connection (fun postgres ->
      let nums = List.init 10000 ~f:Int.to_string |> String.concat ~sep:", " in
      let long_query = "select $1, " ^ nums ^ ", foobar, " ^ nums in
      let%bind result =
        Postgres_async.Expert.query_expect_no_data
          ~parameters:[| Some nums |]
          postgres
          long_query
      in
      print_or_pgasync_error result;
      [%expect
        {|
        (Error
          (error_code (42703))
          (severity   (ERROR))
          (as_error (
            ((query
              "... 29, 9830, 9831, 9832, 9833, 9834, 9835, 9836, 9837, 9838, 9839, 9840, 9841, 9842, 9843, 9844, 9845, 9846, 9847, 9848, 9849, 9850, 9851, 9852, 9853, 9854, 9855, 9856, 9857, 9858, 9859, 9860, 9861, 9862, 9863, 9864, 9865, 9866, 9867, 9868, 9869, 9870, 9871, 9872, 9873, 9874, 9875, 9876, 9877, 9878, 9879, 9880, 9881, 9882, 9883, 9884, 9885, 9886, 9887, 9888, 9889, 9890, 9891, 9892, 9893, 9894, 9895, 9896, 9897, 9898, 9899, 9900, 9901, 9902, 9903, 9904, 9905, 9906, 9907, 9908, 9909, 9910, 9911, 9912, 9913, 9914, 9915, 9916, 9917, 9918, 9919, 9920, 9921, 9922, 9923, 9924, 9925, 9926, 9927, 9928, 9929, 9930, 9931, 9932, 9933, 9934, 9935, 9936, 9937, 9938, 9939, 9940, 9941, 9942, 9943, 9944, 9945, 9946, 9947, 9948, 9949, 9950, 9951, 9952, 9953, 9954, 9955, 9956, 9957, 9958, 9959, 9960, 9961, 9962, 9963, 9964, 9965, 9966, 9967, 9968, 9969, 9970, 9971, 9972, 9973, 9974, 9975, 9976, 9977, 9978, 9979, 9980, 9981, 9982, 9983, 9984, 9985, 9986, 9987, 9988, 9989, 9990, 9991, 9992, 9993, 9994, 9995, 9996, 9997, 9998, 9999, foobar, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 2 ...")
             (parameters ((
               "0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34..."))))
            ("Postgres Server Error (state=Parsing)" ((Code 42703))))))
        |}];
      return ())
  in
  print_or_pgasync_error connection_result;
  [%expect {| (Ok ()) |}];
  return ()
;;
