open Core
open! Int.Replace_polymorphic_compare

(*_ see postgres_async.mli for documentation. *)

type t =
  | Disable
  | Prefer
  | Require
[@@deriving sexp_of]

val to_libpq_string : t -> string
val of_libpq_string : string -> t option
