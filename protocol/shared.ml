open! Core
open! Async
open! Import

(* When [Word_size.word_size = W32], [int] can take at most 31 bits, so the max is 2**30.

   [Iobuf.Consume.int32_be] silently truncates. This is a shame (ideally it would return a
   [Int63.t]; see the comment at the top of [Int32.t], but otherwise an [Int32.t] would
   certainly suffice) and makes it quite annoying to safely implement something that reads
   32 bit ints on a 32 bit ocaml platform.

   Looking below, the 32 bit ints are

   - [message_length]: we'd refuse to read messages larger than 2**30 anyway,
   - [num_fields] (in a row): guaranteed to be < 1600 by postgres
     (https://www.postgresql.org/docs/11/ddl-basics.html),
   - [pid]: on linux, less than 2**22 (man 5 proc),
   - [secret] (from [BackendKeyData]): no guarantees.

   so, for now it seems safe enough to to stumble on in 32-bit-mode even though iobuf
   would silently truncate the ints. This is unsatisfying (besides not supporting reading
   [secret]) because if we've made a mistake, or have a bug, we'd rather crash on the
   protocol error than truncate.

   We'll revisit it if someone wants it. *)

module Non_zero_char = struct
  type t = char [@@deriving compare, equal, hash, quickcheck, sexp]

  let quickcheck_generator = Char.gen_uniform_inclusive '\x01' Char.max_value
end

module Null_terminated_string = struct
  module T = struct
    type t = string [@@deriving compare, equal, hash, quickcheck, sexp]
    type comparator_witness = String.comparator_witness

    let comparator = String.comparator
    let quickcheck_generator = String.gen' [%quickcheck.generator: Non_zero_char.t]

    let validate t =
      match String.mem t '\x00' with
      | false -> Validate.pass
      | true -> Validate.fail "string may not contain nulls"
    ;;

    let payload_length t = String.length t + 1

    let fill iobuf t =
      Iobuf.Fill.stringo iobuf t;
      Iobuf.Fill.char iobuf '\x00'
    ;;

    let consume_exn iobuf =
      match Iobuf.Peek.index iobuf '\x00' with
      | None ->
        raise
          (Not_found_s
             [%sexp
               "Null_terminated_string.consume_exn: could not find terminating null byte"])
      | Some len ->
        let t = Iobuf.Consume.stringo iobuf ~len in
        [%test_result: char] (Iobuf.Consume.char iobuf) ~expect:'\x00';
        t
    ;;
  end

  include T

  module Nonempty = struct
    include T

    let quickcheck_generator =
      String.gen_nonempty' [%quickcheck.generator: Non_zero_char.t]
    ;;

    let validate t =
      if String.is_empty t then Validate.fail "string may not be empty" else validate t
    ;;

    let consume_exn iobuf =
      match consume_exn iobuf with
      | "" -> Error `Empty_string
      | s -> Ok s
    ;;
  end
end

let validate_null_terminated_exn ~field_name str =
  if String.mem str '\x00'
  then raise_s [%message "String may not contain nulls" field_name str]
;;

let fill_null_terminated iobuf str =
  Iobuf.Fill.stringo iobuf str;
  Iobuf.Fill.char iobuf '\x00'
;;

(* Type int16 could be used as both unsigned and signed, depending on the context (See
   pqPutInt/pgGetInt in the interfaces/libpq/fe-misc.c).

   For example, data type size in RowDescription could be negative, but parameter count in
   Bind message could only be positive, and so it's maximum value is 65535. Our
   implementation currently only needs unsigned int16 values *)
let uint16_min = 0
let uint16_max = 65535

let int32_min =
  match Word_size.word_size with
  | W64 -> Int32.to_int_exn Int32.min_value
  | W32 -> Int.min_value
;;

let int32_max =
  match Word_size.word_size with
  | W64 -> Int32.to_int_exn Int32.max_value
  | W32 -> Int.max_value
;;

let () =
  match Word_size.word_size with
  | W64 ->
    assert (String.equal (Int.to_string int32_min) "-2147483648");
    assert (String.equal (Int.to_string int32_max) "2147483647")
  | W32 ->
    assert (String.equal (Int.to_string int32_min) "-1073741824");
    assert (String.equal (Int.to_string int32_max) "1073741823")
;;

let[@inline always] fill_uint16_be iobuf value =
  match uint16_min <= value && value <= uint16_max with
  | true -> Iobuf.Fill.uint16_be_trunc iobuf value
  | false -> failwithf "uint16 out of range: %i" value ()
;;

let[@inline always] fill_int32_be iobuf value =
  match int32_min <= value && value <= int32_max with
  | true -> Iobuf.Fill.int32_be_trunc iobuf value
  | false -> failwithf "int32 out of range: %i" value ()
;;

let find_null_exn iobuf =
  let rec loop ~iobuf ~length ~pos =
    if Char.( = ) (Iobuf.Peek.char iobuf ~pos) '\x00'
    then pos
    else if pos > length - 1
    then failwith "find_null_exn could not find \\x00"
    else loop ~iobuf ~length ~pos:(pos + 1)
  in
  loop ~iobuf ~length:(Iobuf.length iobuf) ~pos:0
;;

let consume_cstring_exn iobuf =
  let len = find_null_exn iobuf in
  let res = Iobuf.Consume.string iobuf ~len ~str_pos:0 in
  let zero = Iobuf.Consume.char iobuf in
  assert (Char.( = ) zero '\x00');
  res
;;

module type Message_type = sig
  val message_type_char : char option

  type t

  val validate_exn : t -> unit
  val payload_length : t -> int
  val fill : t -> (read_write, Iobuf.seek, Iobuf.global) Iobuf.t -> unit
end

type 'a with_computed_length =
  { payload_length : int
  ; value : 'a
  }

let write_message (type a) (module M : Message_type with type t = a) =
  let full_length { payload_length; _ } =
    match M.message_type_char with
    | None -> payload_length + 4
    | Some _ -> payload_length + 5
  in
  let blit_to_bigstring with_computed_length bigstring ~pos =
    let iobuf =
      Iobuf.of_bigstring bigstring ~pos ~len:(full_length with_computed_length)
    in
    (match M.message_type_char with
     | None -> ()
     | Some c -> Iobuf.Fill.char iobuf c);
    let { payload_length; value } = with_computed_length in
    fill_int32_be iobuf (payload_length + 4);
    M.fill value iobuf;
    match Iobuf.is_empty iobuf with
    | true -> ()
    | false -> failwith "postgres message filler lied about length"
  in
  Staged.stage (fun writer value ->
    M.validate_exn value;
    let payload_length = M.payload_length value in
    Async.Writer.write_gen_whole
      writer
      { payload_length; value }
      ~length:full_length
      ~blit_to_bigstring)
;;

module No_arg = struct
  let gen ~constructor =
    let tmp = Iobuf.create ~len:5 in
    Iobuf.Poke.char tmp ~pos:0 constructor;
    (* fine to use Iobuf's int32 function, as [4] is clearly in range. *)
    Iobuf.Poke.int32_be_trunc tmp ~pos:1 4;
    Iobuf.to_string tmp
  ;;
end

(* Both the backend and frontend use the same format for [CopyData] and [CopyDone]
   messages, hence they are placed in [Shared]. *)
module CopyData = struct
  let message_type_char = Some 'd'

  type t = string

  (* After [focus_on_message] seeks over the type and length, 'CopyData' messages are
     simply just the payload bytes. *)
  let skip iobuf = Iobuf.advance iobuf (Iobuf.length iobuf)
  let validate_exn (_ : t) = ()
  let payload_length t = String.length t
  let fill t iobuf = Iobuf.Fill.stringo iobuf t
end

module CopyDone = struct
  let consume (_ : _ Iobuf.t) = ()
end
