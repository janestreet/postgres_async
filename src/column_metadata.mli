open! Core

(*_ see postgres_async.mli for documentation. *)

type t

val create : name:string -> format:[ `Text ] -> pg_type_oid:int -> t
val pg_type_oid : t -> int
val name : t -> string
val format : t -> [ `Text ]
