open! Core

(** Contains information on the name and type of a column in query results *)
type t

val create : name:string -> format:[ `Text ] -> pg_type_oid:int -> t
val pg_type_oid : t -> int
val name : t -> string
val format : t -> [ `Text ]
