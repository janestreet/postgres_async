open! Core

type t =
  { name : string
  ; format : Types.Format.t
  ; pg_type_oid : int
  }
[@@deriving fields ~getters ~iterators:create, sexp_of]

let create = Fields.create

module type Public = Column_metadata_intf.Public with type t = t
