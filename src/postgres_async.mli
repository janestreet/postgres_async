open Core
open! Async

module Pgasync_error : sig
  (** The type [Pgasync_error.t] is used for all errors in this library.

      [to_error] returns a regular [Core.Error.t] that fully describes what went wrong
      (including but not limited to a postgres error code, if we hae one), and if you only
      need to show the error to someone/raise/fail then using that completely suffices.

      _If_ the error due to an [ErrorResponse] from the server _and_ we successfully
      parsed an error code out of it, you can retrieve that via [postgres_error_code].

      Note that either of those conditions might be false, e.g. the error might be due to
      TCP connection failure, so we certainly won't have any postgres error code; you
      shouldn't take the name of the type to mean "postgres error", rather "postgres async
      error"---any error this library can produce. *)
  type t [@@deriving sexp_of]

  val to_error : t -> Error.t
  val postgres_error_code : t -> string option
  val raise : t -> 'a

  module Postgres_field : sig
    type other = private char

    type t =
      | Severity
      | Severity_non_localised
      | Code
      | Message
      | Detail
      | Hint
      | Position
      | Internal_position
      | Internal_query
      | Where
      | Schema
      | Table
      | Column
      | Data_type
      | Constraint
      | File
      | Line
      | Routine
      | Other of other
    [@@deriving sexp_of]
  end

  val postgres_field : t -> Postgres_field.t -> string option
end

module Or_pgasync_error : sig
  type 'a t = ('a, Pgasync_error.t) Result.t [@@deriving sexp_of]

  val to_or_error : 'a t -> 'a Or_error.t
  val ok_exn : 'a t -> 'a
end

module Column_metadata : sig
  (** Contains information on the name and type of a column in query results *)
  type t

  val name : t -> string

  (** Oid of the type of data in the column. To get full type information for
      some [pg_type_oid t = K], [select * from pg_type where oid = K]. *)
  val pg_type_oid : t -> int
end

module Ssl_mode : sig
  (** [t] is a subset of the types supported by the 'sslmode' parameter in libpq
      (documented at https://www.postgresql.org/docs/current/libpq-ssl.html).

      We don't currently support verifying certificate signatures, so there's nothing
      analogous to the "verify-ca" or "verify-full" options here. We don't distinguish
      between "allow" and "prefer" (they seem to exactly match in terms of behavior).

      Under the hood, [Prefer] and [Require] will both result in the very first message
      sent to the server being the [SSLRequest] message, instead of the [StartupMessage].
      The server will respond with whether or not it is able to support SSL.

      Using [Require], [connect] will return an error if the server cannot support SSL.

      [Prefer] will SSL-wrap the connection if the server supports SSL, or will use
      a plain TCP connection if the server does not support SSL.

      [Disable] will always use the plain TCP connection, and will not send the
      [SSLRequest] message. *)
  type t =
    | Disable
    | Prefer
    | Require

  val to_libpq_string : t -> string
  val of_libpq_string : string -> t option
end

type t [@@deriving sexp_of]

include
  Postgres_async_intf.S
  with type t := t
   and type error := Error.t
   and type column_metadata := Column_metadata.t
   and type ssl_mode := Ssl_mode.t

(** The [Expert] module provides versions of all the same functions that instead return
    [Or_pgasync_error.t]s.

    Note that [t] and [Expert.t] is the same type, so you can mix-and-match depending on
    whether you want to try and inspect the error code of a specific failure or not. *)
module Expert :
  Postgres_async_intf.S
  with type t := t
   and type error := Pgasync_error.t
   and type column_metadata := Column_metadata.t
   and type ssl_mode := Ssl_mode.t

module Private : sig
  module Protocol = Protocol

  val pgasync_error_of_error : Error.t -> Pgasync_error.t
end
