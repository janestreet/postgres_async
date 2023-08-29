open Core
open! Int.Replace_polymorphic_compare

type t =
  { name : string
  ; format : [ `Text ]
  ; pg_type_oid : int
  }
[@@deriving fields ~getters ~iterators:create]

let create = Fields.create
