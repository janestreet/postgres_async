open! Core

type t =
  { name : string
  ; format : [ `Text ]
  ; pg_type_oid : int
  }
[@@deriving fields ~getters ~iterators:create, sexp_of]

let create = Fields.create

module type Public = Column_metadata_intf.Public with type t = t
