open! Core
open Async

let harness = lazy (Harness.create ())

let query_exn postgres string =
  let%bind result =
    Postgres_async.query_expect_no_data postgres string
  in
  Or_error.ok_exn result;
  return ()

let print_notifications ?saw_notification postgres ~channel =
  let%bind result =
    Postgres_async.listen_to_notifications postgres ~channel ~f:(fun ~payload ->
      Option.iter saw_notification ~f:(fun bvar -> Bvar.broadcast bvar ());
      print_s [%message "notification" ~channel ~payload]
    )
  in
  Or_error.ok_exn result;
  return ()

let%expect_test "listen/notify" =
  let saw_notification = Bvar.create () in
  let print_notifications = print_notifications ~saw_notification in
  let harness = force harness in
  Harness.with_connection_exn harness ~database:"postgres" (fun postgres ->
    let%bind () = print_notifications postgres ~channel:"channel_one" in
    let%bind () = print_notifications postgres ~channel:"channel-2" in
    let%bind () = [%expect {| |}] in
    let sync1 = Bvar.wait saw_notification in
    let%bind () = query_exn postgres "NOTIFY channel_one" in
    let%bind () = sync1 in
    let%bind () = [%expect {| (notification (channel channel_one) (payload "")) |}] in
    let sync2 = Bvar.wait saw_notification in
    let%bind () = query_exn postgres "NOTIFY \"channel-2\", 'hello'" in
    let%bind () = sync2 in
    let%bind () = [%expect {| (notification (channel channel-2) (payload hello)) |}] in
    let sync3 = Bvar.wait saw_notification in
    let%bind () =
      Harness.with_connection_exn harness ~database:"postgres" (fun postgres2 ->
        query_exn postgres2 "NOTIFY channel_one, 'from-another-process'"
      )
    in
    let%bind () = sync3 in
    let%bind () =
      [%expect {| (notification (channel channel_one) (payload from-another-process)) |}]
    in
    let%bind () =
      Harness.with_connection_exn harness ~database:"postgres" (fun postgres2 ->
        let sync4 = Bvar.wait saw_notification in
        let%bind () = query_exn postgres2 "NOTIFY \"channel-2\", 'm1'" in
        let%bind () = sync4 in
        let sync5 = Bvar.wait saw_notification in
        let%bind () = query_exn postgres2 "NOTIFY \"channel-2\", 'm2'" in
        let%bind () = sync5 in
        return ()
      )
    in
    let%bind () =
      [%expect {|
        (notification (channel channel-2) (payload m1))
        (notification (channel channel-2) (payload m2)) |}]
    in
    return ()
  )

let with_other_log_outputs log outputs ~f =
  let before = Log.get_output log in
  Log.set_output log outputs;
  let finally () =
    Log.set_output log before;
    return ()
  in
  Monitor.protect f ~finally

let%expect_test "multiple listeners" =
  Harness.with_connection_exn (force harness) ~database:"postgres" (fun postgres ->
    let b1 = Bvar.create () in
    let%bind () = print_notifications ~saw_notification:b1 postgres ~channel:"a" in
    let b2 = Bvar.create () in
    let%bind () = print_notifications ~saw_notification:b2 postgres ~channel:"a" in
    let b3 = Bvar.create () in
    let%bind () = print_notifications ~saw_notification:b3 postgres ~channel:"a" in
    let i1 = Bvar.wait b1 in
    let i2 = Bvar.wait b2 in
    let i3 = Bvar.wait b3 in
    let%bind () = query_exn postgres "NOTIFY a" in
    let%bind () = i1 in
    let%bind () = i2 in
    let%bind () = i3 in
    [%expect {|
      (notification (channel a) (payload ""))
      (notification (channel a) (payload ""))
      (notification (channel a) (payload "")) |}]
  )

let%expect_test "notify with no listeners" =
  let saw_log_message = Bvar.create () in
  let log_output =
    let flush () = return () in
    Log.Output.create ~flush (fun messages ->
      Bvar.broadcast saw_log_message ();
      Queue.iter messages ~f:(fun msg ->
        match Log.Message.raw_message msg with
        | `Sexp sexp -> print_s [%message "LOG" ~_:(sexp : Sexp.t)]
        | `String str -> printf "LOG: %s str\n" str
      );
      return ()
    )
  in
  with_other_log_outputs (force Log.Global.log) [log_output] ~f:(fun () ->
    Harness.with_connection_exn (force harness) ~database:"postgres" (fun postgres ->
      let sync = Bvar.wait saw_log_message in
      let%bind () = query_exn postgres "LISTEN channel_three" in
      let%bind () = query_exn postgres "NOTIFY channel_three" in
      let%bind () = sync in
      [%expect {|
        (LOG
         ("Postgres NotificationResponse on channel that no callbacks are listening to"
          (channel channel_three))) |}]
    )
  )
