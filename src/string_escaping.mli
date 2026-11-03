open! Core

module Copy_in : sig
  type format =
    | Text
    | Binary

  (** Note that [schema_name], [table_name], and [column_names] must be escaped before
      calling [query]. *)
  val query
    :  ?schema_name:string
    -> ?format:format (** Default is [Text]. *)
    -> table_name:string
    -> column_names:string list
    -> unit
    -> string

  (** [row_to_string] includes the terminating '\n' *)
  val row_to_string : string or_null array -> string
end

module Listen : sig
  val query : channel:string -> string
end

(** No [escape_value] function is provided, because so far parameters have sufficed for
    putting values into query strings. *)
