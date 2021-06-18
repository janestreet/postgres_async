open Core
open Async

let with_connection_exn =
  let database = "test_copy_in" in
  let harness =
    lazy
      (let h = Harness.create () in
       Harness.create_database h database;
       h)
  in
  fun func -> Harness.with_connection_exn (force harness) ~database func
;;

let create_table postgres name columns =
  let%bind result =
    Postgres_async.query_expect_no_data
      postgres
      (sprintf "CREATE TEMPORARY TABLE %s ( %s )" name (String.concat ~sep:"," columns))
  in
  Or_error.ok_exn result;
  return ()
;;

let print_table postgres =
  let%bind result =
    Postgres_async.query
      postgres
      "SELECT * FROM x ORDER BY y"
      ~handle_row:(fun ~column_names:_ ~values ->
        print_s [%sexp (values : string option array)])
  in
  Or_error.ok_exn result;
  return ()
;;

let%expect_test "copy_in_rows" =
  with_connection_exn (fun postgres ->
    let%bind () = create_table postgres "x" [ "y integer primary key"; "z text" ] in
    [%expect {||}];
    let%bind result =
      let rows =
        Queue.of_list
          [ [| Some "one"; Some "1" |]
          ; [| None; Some "2" |]
          ; [| Some "three"; Some "3" |]
          ]
      in
      Postgres_async.copy_in_rows
        postgres
        ~table_name:"x"
        ~column_names:[| "z"; "y" |]
        ~feed_data:(fun () ->
          match Queue.dequeue rows with
          | None -> Finished
          | Some c -> Data c)
    in
    Or_error.ok_exn result;
    [%expect {| |}];
    let%bind () = print_table postgres in
    [%expect {|
      ((1) (one))
      ((2) ())
      ((3) (three)) |}];
    return ())
;;

let%expect_test "copy_in_rows: nasty characters" =
  with_connection_exn (fun postgres ->
    let%bind () =
      create_table postgres "x" [ "y integer primary key"; "z text"; "w bytea" ]
    in
    [%expect {||}];
    let%bind result =
      let rows =
        Queue.of_list
          [ [| Some "1"; Some "\n"; None |]
          ; [| Some "2"; Some "\\N"; None |]
          ; [| Some "3"; Some "\t"; None |]
          ; [| Some "4"; Some "\\t"; None |]
          ; [| Some "5"; Some ","; None |]
          ; [| Some "6"; Some ""; None |]
          ; [| Some "7"; Some "\\"; None |]
          ; [| Some "8"; Some "\\x61"; None |]
          ; [| Some "9"; Some ""; None |]
          ; [| Some "10"; Some "\x00"; None |]
          ; [| Some "11"; None; Some "asdf" |]
          ; [| Some "12"; None; Some "\n" |]
          ; [| Some "13"; None; Some "\\x00" |]
          ; [| Some "14"; None; Some "\\x61" |]
          ]
      in
      Postgres_async.copy_in_rows
        postgres
        ~table_name:"x"
        ~column_names:[| "y"; "z"; "w" |]
        ~feed_data:(fun () ->
          match Queue.dequeue rows with
          | None -> Finished
          | Some c -> Data c)
    in
    Or_error.ok_exn result;
    [%expect {| |}];
    let%bind () = print_table postgres in
    [%expect
      {|
      ((1) ("\n") ())
      ((2) ("\\N") ())
      ((3) ("\t") ())
      ((4) ("\\t") ())
      ((5) (,) ())
      ((6) ("") ())
      ((7) ("\\") ())
      ((8) ("\\x61") ())
      ((9) ("") ())
      ((10) ("") ())
      ((11) () ("\\x61736466"))
      ((12) () ("\\x0a"))
      ((13) () ("\\x00"))
      ((14) () ("\\x61")) |}];
    return ())
;;

let%expect_test "copy_in_rows: nasty column names" =
  with_connection_exn (fun postgres ->
    let%bind result =
      (* year is a keyword and must be quoted *)
      Postgres_async.query_expect_no_data
        postgres
        {|
          CREATE TEMPORARY TABLE "table-name " (
            k integer primary key,
            "y space" text,
            "z""quote" text,
            "year" text,
            LOWERCASE1 text,
            "UPPERCASE2" text
          )
        |}
    in
    Or_error.ok_exn result;
    [%expect {||}];
    let%bind result =
      let sent_row = ref false in
      Postgres_async.copy_in_rows
        postgres
        ~table_name:"table-name "
        ~column_names:
          [| "k"; "y space"; "z\"quote"; "year"; "lowercase1"; "UPPERCASE2" |]
        ~feed_data:(fun () ->
          match !sent_row with
          | true -> Finished
          | false ->
            sent_row := true;
            Data [| Some "1"; Some "A"; Some "B"; Some "C"; Some "D"; Some "E" |])
    in
    Or_error.ok_exn result;
    [%expect {| |}];
    let%bind result =
      Postgres_async.query
        postgres
        {| SELECT * FROM "table-name " ORDER BY k |}
        ~handle_row:(fun ~column_names ~values ->
          Array.iter (Array.zip_exn column_names values) ~f:(fun (k, v) ->
            print_s [%sexp (k : string), (v : string option)]))
    in
    Or_error.ok_exn result;
    [%expect
      {|
      (k (1))
      ("y space" (A))
      ("z\"quote" (B))
      (year (C))
      (lowercase1 (D))
      (UPPERCASE2 (E)) |}];
    return ())
;;

let%expect_test "copy_in_rows: lots of data" =
  with_connection_exn (fun postgres ->
    let%bind () = create_table postgres "x" [ "y integer primary key"; "z text" ] in
    [%expect {||}];
    let%bind result =
      let counter = ref 0 in
      let sleeps = ref 0 in
      let one_kb = String.init 1024 ~f:(const 'a') in
      Postgres_async.copy_in_rows
        postgres
        ~table_name:"x"
        ~column_names:[| "y"; "z" |]
        ~feed_data:(fun () ->
          match !counter >= 8192 with
          | true -> Finished
          | false ->
            (match !counter / 256 < !sleeps with
             | true ->
               incr sleeps;
               Wait ((force Utils.do_an_epoll) ())
             | false ->
               incr counter;
               Data [| Some (Int.to_string !counter); Some one_kb |]))
    in
    Or_error.ok_exn result;
    [%expect {| |}];
    let%bind result =
      Postgres_async.query
        postgres
        {| SELECT COUNT(*), MIN(y), MAX(y), SUM(y), MIN(LENGTH(z)) FROM x |}
        ~handle_row:(fun ~column_names:_ ~values ->
          let values = Array.map ~f:(fun x -> Option.value_exn x) values in
          print_s [%sexp (values : string array)])
    in
    Or_error.ok_exn result;
    [%expect {| (8192 1 8192 33558528 1024) |}];
    print_s [%sexp (8192 * 8193 / 2 : int)];
    [%expect {| 33558528 |}];
    return ())
;;

let%expect_test "raw: weird chunking" =
  with_connection_exn (fun postgres ->
    let%bind () =
      create_table postgres "x" [ "y integer primary key"; "z text not null" ]
    in
    [%expect {||}];
    (* weird chunking is fine, it doesn't need to correspond to rows: *)
    let%bind result =
      let chunks = Queue.of_list [ "1\tone\n"; "2\t"; "two"; "\n" ] in
      Postgres_async.copy_in_raw
        postgres
        "COPY x (y, z) FROM STDIN"
        ~feed_data:(fun () ->
          match Queue.dequeue chunks with
          | None -> Finished
          | Some c -> Data c)
    in
    Or_error.ok_exn result;
    [%expect {||}];
    let%bind () = print_table postgres in
    [%expect {|
      ((1) (one))
      ((2) (two)) |}];
    return ())
;;

let%expect_test "aborting" =
  with_connection_exn (fun postgres ->
    let%bind () =
      create_table postgres "x" [ "y integer primary key"; "z text not null" ]
    in
    [%expect {||}];
    let%bind result =
      let count = ref 0 in
      Postgres_async.copy_in_raw
        postgres
        "COPY x (y, z) FROM STDIN"
        ~feed_data:(fun () ->
          incr count;
          match !count with
          | 1 -> Data "1\tone\n"
          | 2 -> Wait (Scheduler.yield_until_no_jobs_remain ())
          | 3 -> Data "2\ttwo\n"
          | 4 -> Abort { reason = "user reason" }
          | _ -> assert false)
    in
    (match result with
     | Ok () -> failwith "succeeded!?"
     | Error err ->
       let err = Utils.delete_unstable_bits_of_error [%sexp (err : Error.t)] in
       print_s err);
    (* 57014: query_cancelled. *)
    [%expect {| ((Code 57014)) |}];
    let%bind () = print_table postgres in
    [%expect {| |}];
    return ())
;;
