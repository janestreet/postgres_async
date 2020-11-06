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
end

module Or_pgasync_error : sig
  type 'a t = ('a, Pgasync_error.t) Result.t [@@deriving sexp_of]

  val to_or_error : 'a t -> 'a Or_error.t
  val ok_exn : 'a t -> 'a
end

type t [@@deriving sexp_of]

include Postgres_async_intf.S
  with type t := t
   and type error := Error.t

(** The [Expert] module provides versions of all the same functions that instead return
    [Or_pgasync_error.t]s.

    Note that [t] and [Expert.t] is the same type, so you can mix-and-match depending on
    whether you want to try and inspect the error code of a specific failure or not. *)
module Expert : Postgres_async_intf.S
  with type t := t
   and type error := Pgasync_error.t

module Private : sig
  val pgasync_error_of_error : Error.t -> Pgasync_error.t
end
