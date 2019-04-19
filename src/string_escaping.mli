open! Core

(** [escape_identifier] is for things like table names, column names, ... *)
val escape_identifier : string -> string

module Copy_in : sig
  val query : table_name:string -> column_names:string array -> string

  (** [row_to_string] includes the terminating '\n' *)
  val row_to_string : string option array -> string
end

module Listen : sig
  val query : channel:string -> string
end

(** No [escape_value] function is provided, because so far parameters have
    sufficed for putting values into query strings. *)
