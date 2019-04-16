open Core
open! Int.Replace_polymorphic_compare

type backend_key =
  { pid : Pid.t
  ; secret : int
  }
[@@deriving sexp_of]

module type Named_or_unnamed = sig
  type t

  val unnamed : t

  val create_named_exn : string -> t

  val to_string : t -> string
end

module Named_or_unnamed = struct
  type t = string

  let unnamed = ""

  let create_named_exn s =
    (if String.is_empty s then failwith "Named_or_unnamed.create_named_exn got an empty string");
    (if String.mem s '\x00' then failwith "Named_or_unnamed.create_named_exn: string must not contain \\x00");
    s

  let to_string (t : t) = (t : string)
end

module Statement_name = Named_or_unnamed
module Portal_name = Named_or_unnamed

module Notification_channel = String
