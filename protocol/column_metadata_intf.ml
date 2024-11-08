open! Core

module type Public = sig
  (** Contains information on the name and type of a column in query results *)
  type t

  val name : t -> string

  (** Oid of the type of data in the column. To get full type information for
    some [pg_type_oid t = K], [select * from pg_type where oid = K]. *)
  val pg_type_oid : t -> int
end

module type Column_metadata = sig
  type t [@@deriving sexp_of]

  val create : name:string -> format:[ `Text ] -> pg_type_oid:int -> t

  include Public with type t := t

  module type Public = Public with type t = t
end
