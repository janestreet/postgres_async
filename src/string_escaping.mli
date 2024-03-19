open! Core

module Copy_in : sig
  (** Note that [schema_name], [table_name], and [column_names] must be escaped before
      calling [query]. *)
  val query
    :  ?schema_name:string
    -> table_name:string
    -> column_names:string list
    -> unit
    -> string

  (** [row_to_string] includes the terminating '\n' *)
  val row_to_string : string option array -> string
end

module Listen : sig
  val query : channel:string -> string
end

(** No [escape_value] function is provided, because so far parameters have
    sufficed for putting values into query strings. *)
