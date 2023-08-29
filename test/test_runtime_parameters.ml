open! Core
open Async

let () = Backtrace.elide := true
let harness = lazy (Harness.create ())

let elide_timezone params =
  Map.change params "TimeZone" ~f:(Option.map ~f:(fun _ -> "LOCAL"))
;;

let startup_message
      ?replication
      ?(options = [])
      ?(runtime_parameters = String.Map.empty)
      ()
  =
  Postgres_async.Private.Protocol.Frontend.StartupMessage.
    { user = "postgres"; database = "postgres"; options; runtime_parameters; replication }
;;

let login_and_print_params startup_message =
  let harness = force harness in
  let conn =
    Postgres_async.Private.Without_background_asynchronous_message_handling
    .login_and_get_raw
      ~server:(Harness.where_to_connect harness)
      ~startup_message
      ()
  in
  match%bind conn with
  | Error error ->
    print_s [%message (error : Postgres_async.Pgasync_error.t)];
    Deferred.unit
  | Ok conn ->
    let params =
      Postgres_async.Private.Without_background_asynchronous_message_handling
      .runtime_parameters
        conn
    in
    let params = elide_timezone params in
    print_s [%message (params : string String.Map.t)];
    Postgres_async.Private.Without_background_asynchronous_message_handling.writer conn
    |> Writer.close
;;

let%expect_test "get runtime parameters" =
  let%bind () = login_and_print_params (startup_message ()) in
  [%expect
    {|
    (params
     ((DateStyle "ISO, MDY") (IntervalStyle postgres) (TimeZone LOCAL)
      (application_name "") (client_encoding SQL_ASCII) (integer_datetimes on)
      (is_superuser on) (server_encoding SQL_ASCII) (server_version 12.10)
      (session_authorization postgres) (standard_conforming_strings on))) |}];
  return ()
;;

let%expect_test "set runtime parameters" =
  let%bind () =
    startup_message
      ~runtime_parameters:(String.Map.singleton "application_name" "simple_app")
      ()
    |> login_and_print_params
  in
  [%expect
    {|
    (params
     ((DateStyle "ISO, MDY") (IntervalStyle postgres) (TimeZone LOCAL)
      (application_name simple_app) (client_encoding SQL_ASCII)
      (integer_datetimes on) (is_superuser on) (server_encoding SQL_ASCII)
      (server_version 12.10) (session_authorization postgres)
      (standard_conforming_strings on))) |}];
  return ()
;;

let%expect_test "set options" =
  let%bind () =
    startup_message ~options:[ "--application_name=My \\complicated  appname" ] ()
    |> login_and_print_params
  in
  [%expect
    {|
    (params
     ((DateStyle "ISO, MDY") (IntervalStyle postgres) (TimeZone LOCAL)
      (application_name "My \\complicated  appname") (client_encoding SQL_ASCII)
      (integer_datetimes on) (is_superuser on) (server_encoding SQL_ASCII)
      (server_version 12.10) (session_authorization postgres)
      (standard_conforming_strings on))) |}];
  return ()
;;
