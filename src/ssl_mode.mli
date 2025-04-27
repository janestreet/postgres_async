open Core
open! Int.Replace_polymorphic_compare

(** [t] is a subset of the types supported by the 'sslmode' parameter in libpq (documented
    at https://www.postgresql.org/docs/current/libpq-ssl.html).

    We don't currently support verifying certificate signatures, so there's nothing
    analogous to the "verify-ca" or "verify-full" options here. We don't distinguish
    between "allow" and "prefer" (they seem to exactly match in terms of behavior).

    Under the hood, [Prefer] and [Require] will both result in the very first message sent
    to the server being the [SSLRequest] message, instead of the [StartupMessage]. The
    server will respond with whether or not it is able to support SSL.

    Using [Require], [connect] will return an error if the server cannot support SSL.

    [Prefer] will SSL-wrap the connection if the server supports SSL, or will use a plain
    TCP connection if the server does not support SSL.

    [Disable] will always use the plain TCP connection, and will not send the [SSLRequest]
    message. *)

type t =
  | Disable
  | Prefer
  | Require
[@@deriving sexp_of]

val to_libpq_string : t -> string
val of_libpq_string : string -> t option
