open Core

type backend_key =
  { pid : Pid.t
  ; secret : int
  }
[@@deriving sexp_of]

module type Named_or_unnamed = sig
  type t

  val unnamed : t

  (** The provided string must be nonempty and not contain nulls. *)
  val create_named_exn : string -> t

  (** [to_string unnamed = ""] *)
  val to_string : t -> string
end

module Statement_name : Named_or_unnamed
module Portal_name : Named_or_unnamed

module Notification_channel : sig
  type t [@@deriving sexp_of]
  include Stringable with type t := t
  include Hashable with type t := t
end
