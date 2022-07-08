open Core
open! Int.Replace_polymorphic_compare

type t =
  | Disable
  | Prefer
  | Require
[@@deriving sexp_of, variants]

let of_libpq_string string =
  match String.lowercase string with
  | "disable" -> Some Disable
  | "prefer" -> Some Prefer
  | "require" -> Some Require
  | _ -> None
;;

let to_libpq_string t = Variants.to_name t |> String.lowercase
