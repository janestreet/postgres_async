open Core

type backend_key =
  { pid : int
  (** The type of [pid] is intentionally [int] instead of [Core.Pid.t].

      When the client is connected directly to a database cluster this field is expected
      to be a Linux PID, in which case the assumptions in internal validation of
      [Core.Pid] are valid: in particular, that a PID must be a positive number.

      In the case that a client is connected to a
      {{:https://www.pgbouncer.org/} pgbouncer} proxy the PID supplied (and tracked) by
      the proxy is random. This is done to allow cancel requests per proxied client.

      The postgres protocol spec states that this field is "Int32: The process ID of this
      backend", so it's a little unclear as to whether or not pgbouncer's behaviour is a
      violation of the spec. In any case, it is what it is, and this field is an int
      rather than a [Pid.t] in order to support connecting via pgbouncer.

      This may also be true of other proxies or similar software. *)
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
