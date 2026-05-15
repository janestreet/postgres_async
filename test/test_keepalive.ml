open! Core
open Async

let harness = lazy (Harness.create ())

let%expect_test "tcp connections have SO_KEEPALIVE set" =
  let harness = force harness in
  let hap = Host_and_port.create ~host:"127.0.0.1" ~port:(Harness.port harness) in
  let%bind result =
    Postgres_async.with_connection
      ~server:(Tcp.Where_to_connect.of_host_and_port hap)
      ~user:"postgres"
      ~database:"postgres"
      ~on_handler_exception:`Raise
      (fun postgres ->
         let fd = Postgres_async.Private.writer postgres |> Writer.fd in
         let keepalive =
           Fd.with_file_descr_exn fd (fun file_descr ->
             Core_unix.getsockopt file_descr SO_KEEPALIVE)
         in
         print_s [%message (keepalive : bool)];
         return ())
  in
  Or_error.ok_exn result;
  [%expect {| (keepalive true) |}];
  return ()
;;
