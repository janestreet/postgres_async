open! Core
open! Async
open! Import

module Non_zero_char : sig
  type t = char [@@deriving compare, equal, hash, quickcheck, sexp]
end

module Null_terminated_string : sig
  type t = string [@@deriving compare, equal, hash, quickcheck, sexp]
  type comparator_witness = String.comparator_witness

  include Comparator.S with type t := t and type comparator_witness := comparator_witness

  val validate : t Validate.check
  val payload_length : t -> int
  val fill : (t, read_write, seek, Iobuf.global) Iobuf.Fill.t
  val consume_exn : (t, [> read ], seek, Iobuf.global) Iobuf.Consume.t

  module Nonempty : sig
    type nonrec t = t [@@deriving compare, equal, hash, quickcheck, sexp]

    include Comparator.S with type t := t and type comparator_witness = comparator_witness

    val validate : t -> Validate.t
    val payload_length : t -> int
    val fill : (t, read_write, seek, Iobuf.global) Iobuf.Fill.t

    val consume_exn
      : ((t, [> `Empty_string ]) result, [> read ], seek, Iobuf.global) Iobuf.Consume.t
  end
end

val validate_null_terminated_exn : field_name:string -> string -> unit
val fill_null_terminated : (read_write, seek) Iobuf.t -> string -> unit
val uint16_min : int
val uint16_max : int
val int32_min : int
val int32_max : int
val fill_uint16_be : (read_write, seek) Iobuf.t -> int -> unit
val fill_int32_be : (read_write, seek) Iobuf.t -> int -> unit
val find_null_exn : ([> read ], 'a) Iobuf.t -> int
val consume_cstring_exn : ([> read ], seek) Iobuf.t -> string

module type Message_type = sig
  val message_type_char : char option

  type t

  val validate_exn : t -> unit
  val payload_length : t -> int
  val fill : t -> (read_write, seek) Iobuf.t -> unit
end

val write_message
  :  (module Message_type with type t = 'a)
  -> (Writer.t -> 'a -> unit) Staged.t

module No_arg : sig
  val gen : constructor:char -> string
end

module CopyData : sig
  include Message_type with type t = string

  val message_type_char : char option
  val skip : ([> read ], seek) Iobuf.t -> unit
end

module CopyDone : sig
  val consume : ([> read ], seek) Iobuf.t -> unit
end
