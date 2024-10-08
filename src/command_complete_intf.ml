open! Core

module type Public = sig
  type t [@@deriving sexp_of]

  (** [tag] is a short description of the command that was completed, like "SELECT",
      "BEGIN", "INSERT" or "CREATE TABLE", see description of the CommandComplete message
      in https://www.postgresql.org/docs/current/protocol-message-formats.html *)
  val tag : t -> string

  (** [rows] returns the affected rows count for the completed command, if available and
      was provided in the CommandComplete message *)
  val rows : t -> int option
end

module type Command_complete = sig
  include Public

  val create : tag:string -> rows:int option -> t

  (** The value we return for the empty query *)
  val empty : t

  module type Public = Public with type t = t
end
