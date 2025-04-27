open! Core
open! Async

let setup =
  [ "CREATE EXTENSION IF NOT EXISTS hstore"
  ; "BEGIN"
  ; {|
CREATE TABLE example_datatypes (
    "null" text,
    "bit" bit,
    "bit varying" bit varying,
    "char" char,
    "int2" int2,
    "int4" int4,
    "int8" int8,
    "float4" float4,
    "float8" float8,
    "uuid" uuid,
    "numeric" numeric,
    "array" integer[],
    "cidr" cidr,
    "macaddr" macaddr,
    "jsonb" jsonb,
    "hstore" hstore,
    "text" text,
    "bytea" bytea,
    "varchar" varchar(20)
)|}
  ; {|
INSERT INTO example_datatypes (
    "null", "bit", "bit varying", "char", "int2", "int4", "int8", "float4", "float8",
    "uuid", "numeric", "array", "cidr", "macaddr", "jsonb", "hstore",
    "text", "bytea", "varchar"
) VALUES (
    NULL,
    B'1',
    B'11010001000000010000000000000001000000000000000000000000000000010',
    'A',
    32767,
    2147483647,
    9223372036854775807,
    2.7182817,
    3.14159265358979::float8,
    '123e4567-e89b-12d3-a456-426614174000',
    123.45,
    ARRAY[1, 2, 3],
    '192.168.100.128/25',
    '08:00:2b:01:02:03',
    '{"key": "value", "nested": {"array": [1, 2, 3]}}'::jsonb,
    '"key1"=>"value1", "key2"=>"value2"'::hstore,
    'This is a text field',
    E'\\x00DEADBEEF00'::bytea,
    'VARCHAR example'
)
|}
  ; "COMMIT"
  ]
;;

let with_connection_exn =
  let database = "test_copy_out" in
  let harness =
    lazy
      (let h = Harness.create () in
       Harness.create_database h database;
       h)
  in
  fun func -> Harness.with_connection_exn (force harness) ~database func
;;

let copy_out postgres query_string =
  let messages = Queue.create () in
  let%map command_complete =
    Postgres_async.Private.iter_copy_out postgres ~query_string ~f:(fun iobuf ->
      Queue.enqueue messages (Iobuf.Consume.stringo iobuf);
      return ())
    >>| Postgres_async.Or_pgasync_error.ok_exn
  in
  print_s [%sexp (command_complete : Postgres_async.Command_complete.t)];
  messages
;;

let%expect_test "setup" =
  with_connection_exn (fun postgres ->
    Deferred.List.iter ~how:`Sequential setup ~f:(fun command ->
      Postgres_async.query_expect_no_data postgres command >>| ok_exn))
;;

let%expect_test "empty" =
  with_connection_exn (fun postgres ->
    let%bind messages = copy_out postgres "COPY (SELECT) TO STDOUT" in
    [%expect {| ((tag "COPY 1") (rows ())) |}];
    Queue.iter messages ~f:(fun m -> print_endline (String.Hexdump.to_string_hum m));
    [%expect {| 00000000  0a                                                |.| |}];
    let%bind messages = copy_out postgres "COPY (SELECT) TO STDOUT (FORMAT binary)" in
    [%expect {| ((tag "COPY 1") (rows ())) |}];
    Queue.iter messages ~f:(fun m -> print_endline (String.Hexdump.to_string_hum m));
    [%expect
      {|
      00000000  50 47 43 4f 50 59 0a ff  0d 0a 00 00 00 00 00 00  |PGCOPY..........|
      00000010  00 00 00 00 00                                    |.....|
      00000000  ff ff                                             |..|
      |}];
    return ())
;;

let%expect_test "example" =
  with_connection_exn (fun postgres ->
    let%bind messages = copy_out postgres "COPY example_datatypes TO STDOUT" in
    [%expect {| ((tag "COPY 1") (rows ())) |}];
    Queue.iter messages ~f:(fun m -> print_endline (String.Hexdump.to_string_hum m));
    [%expect
      {xxx|
      00000000  5c 4e 09 31 09 31 31 30  31 30 30 30 31 30 30 30  |\N.1.11010001000|
      00000010  30 30 30 30 31 30 30 30  30 30 30 30 30 30 30 30  |0000100000000000|
      00000020  30 30 30 30 31 30 30 30  30 30 30 30 30 30 30 30  |0000100000000000|
      00000030  30 30 30 30 30 30 30 30  30 30 30 30 30 30 30 30  |0000000000000000|
      00000040  30 30 30 30 31 30 09 41  09 33 32 37 36 37 09 32  |000010.A.32767.2|
      00000050  31 34 37 34 38 33 36 34  37 09 39 32 32 33 33 37  |147483647.922337|
      00000060  32 30 33 36 38 35 34 37  37 35 38 30 37 09 32 2e  |2036854775807.2.|
      00000070  37 31 38 32 38 31 37 09  33 2e 31 34 31 35 39 32  |7182817.3.141592|
      00000080  36 35 33 35 38 39 37 39  09 31 32 33 65 34 35 36  |65358979.123e456|
      00000090  37 2d 65 38 39 62 2d 31  32 64 33 2d 61 34 35 36  |7-e89b-12d3-a456|
      000000a0  2d 34 32 36 36 31 34 31  37 34 30 30 30 09 31 32  |-426614174000.12|
      000000b0  33 2e 34 35 09 7b 31 2c  32 2c 33 7d 09 31 39 32  |3.45.{1,2,3}.192|
      000000c0  2e 31 36 38 2e 31 30 30  2e 31 32 38 2f 32 35 09  |.168.100.128/25.|
      000000d0  30 38 3a 30 30 3a 32 62  3a 30 31 3a 30 32 3a 30  |08:00:2b:01:02:0|
      000000e0  33 09 7b 22 6b 65 79 22  3a 20 22 76 61 6c 75 65  |3.{"key": "value|
      000000f0  22 2c 20 22 6e 65 73 74  65 64 22 3a 20 7b 22 61  |", "nested": {"a|
      00000100  72 72 61 79 22 3a 20 5b  31 2c 20 32 2c 20 33 5d  |rray": [1, 2, 3]|
      00000110  7d 7d 09 22 6b 65 79 31  22 3d 3e 22 76 61 6c 75  |}}."key1"=>"valu|
      00000120  65 31 22 2c 20 22 6b 65  79 32 22 3d 3e 22 76 61  |e1", "key2"=>"va|
      00000130  6c 75 65 32 22 09 54 68  69 73 20 69 73 20 61 20  |lue2".This is a |
      00000140  74 65 78 74 20 66 69 65  6c 64 09 5c 5c 78 30 30  |text field.\\x00|
      00000150  64 65 61 64 62 65 65 66  30 30 09 56 41 52 43 48  |deadbeef00.VARCH|
      00000160  41 52 20 65 78 61 6d 70  6c 65 0a                 |AR example.|
      |xxx}];
    let%bind messages =
      copy_out postgres "COPY example_datatypes TO STDOUT (FORMAT binary)"
    in
    [%expect {| ((tag "COPY 1") (rows ())) |}];
    Queue.iter messages ~f:(fun m -> print_endline (String.Hexdump.to_string_hum m));
    [%expect
      {|
      00000000  50 47 43 4f 50 59 0a ff  0d 0a 00 00 00 00 00 00  |PGCOPY..........|
      00000010  00 00 00 00 13 ff ff ff  ff 00 00 00 05 00 00 00  |................|
      00000020  01 80 00 00 00 0d 00 00  00 41 d1 01 00 01 00 00  |.........A......|
      00000030  00 01 00 00 00 00 01 41  00 00 00 02 7f ff 00 00  |.......A........|
      00000040  00 04 7f ff ff ff 00 00  00 08 7f ff ff ff ff ff  |................|
      00000050  ff ff 00 00 00 04 40 2d  f8 54 00 00 00 08 40 09  |......@-.T....@.|
      00000060  21 fb 54 44 2d 11 00 00  00 10 12 3e 45 67 e8 9b  |!.TD-......>Eg..|
      00000070  12 d3 a4 56 42 66 14 17  40 00 00 00 00 0c 00 02  |...VBf..@.......|
      00000080  00 00 00 00 00 02 00 7b  11 94 00 00 00 2c 00 00  |.......{.....,..|
      00000090  00 01 00 00 00 00 00 00  00 17 00 00 00 03 00 00  |................|
      000000a0  00 01 00 00 00 04 00 00  00 01 00 00 00 04 00 00  |................|
      000000b0  00 02 00 00 00 04 00 00  00 03 00 00 00 08 02 19  |................|
      000000c0  01 04 c0 a8 64 80 00 00  00 06 08 00 2b 01 02 03  |....d.......+...|
      000000d0  00 00 00 31 01 7b 22 6b  65 79 22 3a 20 22 76 61  |...1.{"key": "va|
      000000e0  6c 75 65 22 2c 20 22 6e  65 73 74 65 64 22 3a 20  |lue", "nested": |
      000000f0  7b 22 61 72 72 61 79 22  3a 20 5b 31 2c 20 32 2c  |{"array": [1, 2,|
      00000100  20 33 5d 7d 7d 00 00 00  28 00 00 00 02 00 00 00  | 3]}}...(.......|
      00000110  04 6b 65 79 31 00 00 00  06 76 61 6c 75 65 31 00  |.key1....value1.|
      00000120  00 00 04 6b 65 79 32 00  00 00 06 76 61 6c 75 65  |...key2....value|
      00000130  32 00 00 00 14 54 68 69  73 20 69 73 20 61 20 74  |2....This is a t|
      00000140  65 78 74 20 66 69 65 6c  64 00 00 00 06 00 de ad  |ext field.......|
      00000150  be ef 00 00 00 00 0f 56  41 52 43 48 41 52 20 65  |.......VARCHAR e|
      00000160  78 61 6d 70 6c 65                                 |xample|
      00000000  ff ff                                             |..|
      |}];
    return ())
;;

let show_query postgres query =
  Postgres_async.query postgres query ~handle_row:(fun ~column_names ~values ->
    let values = Iarray.zip_exn column_names values in
    print_s [%sexp (values : (string * string option) iarray)])
  >>| ok_exn
;;

let%expect_test "round-trip" =
  with_connection_exn (fun postgres ->
    let%bind () =
      let%bind messages = copy_out postgres "COPY example_datatypes TO STDOUT" in
      [%expect {| ((tag "COPY 1") (rows ())) |}];
      Postgres_async.copy_in_raw
        postgres
        "COPY example_datatypes FROM STDIN"
        ~feed_data:(fun () ->
          match Queue.dequeue messages with
          | None -> Finished
          | Some x -> Data x)
      >>| ok_exn
    in
    let%bind () =
      let%bind messages =
        copy_out postgres "COPY example_datatypes TO STDOUT (FORMAT binary)"
      in
      [%expect {| ((tag "COPY 2") (rows ())) |}];
      Postgres_async.copy_in_raw
        postgres
        "COPY example_datatypes FROM STDIN (FORMAT binary)"
        ~feed_data:(fun () ->
          match Queue.dequeue messages with
          | None -> Finished
          | Some x -> Data x)
      >>| ok_exn
    in
    let%bind () = show_query postgres "SELECT count(*) FROM example_datatypes" in
    [%expect {| ((count (4))) |}];
    let%bind () =
      show_query
        postgres
        "SELECT count(*) FROM (SELECT DISTINCT * from example_datatypes) as x"
    in
    (* we re-inserted the exact same row *)
    [%expect {| ((count (1))) |}];
    return ())
;;
