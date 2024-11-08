open! Core

type t =
  { tag : string
  ; rows : int option
  }
[@@deriving fields ~getters ~iterators:create, sexp_of]

let create = Fields.create
let empty = { tag = ""; rows = None }

module type Public = Command_complete_intf.Public with type t = t
