open! Core
module ErrorResponse = Postgres_async_protocol.Backend.ErrorResponse
module Postgres_field = Postgres_async_protocol.Backend.Error_or_notice_field

module Sqlstate : sig
  (** PostgreSQL Error Codes.

      Excerpt from {{:https://www.postgresql.org/docs/current/errcodes-appendix.html}
      PostgreSQL Error Codes}:

      "All messages emitted by the PostgreSQL server are assigned five-character error
      codes that follow the SQL standard's conventions for “SQLSTATE” codes. Applications
      that need to know which error condition has occurred should usually test the error
      code, rather than looking at the textual error message. The error codes are less
      likely to change across PostgreSQL releases, and also are not subject to change due
      to localization of error messages. Note that some, but not all, of the error codes
      produced by PostgreSQL are defined by the SQL standard; some additional error codes
      for conditions not defined by the standard have been invented or borrowed from other
      databases."

      We replicate some of these error codes here for [Postgres_async] to report in its
      own error codes when appropriate.

      See also:
      {{:https://github.com/postgres/postgres/blob/master/src/backend/utils/errcodes.txt}
      src/backend/utils/errcodes.txt} *)

  type t = private string [@@deriving compare, equal, hash, sexp_of]

  val connection_exception : t
  val sqlclient_unable_to_establish_sqlconnection : t
  val connection_does_not_exist : t
  val sqlserver_rejected_establishment_of_sqlconnection : t
  val connection_failure : t
  val protocol_violation : t
  val invalid_password : t
  val invalid_authorization_specification : t
end

type t [@@deriving sexp_of]

val of_error : ?error_code:Sqlstate.t -> Error.t -> t
val of_exn : ?error_code:Sqlstate.t -> exn -> t
val of_string : ?error_code:Sqlstate.t -> string -> t
val create_s : ?error_code:Sqlstate.t -> Sexp.t -> t
val of_error_response : ErrorResponse.t -> t
val tag : t -> tag:string -> t
val to_error : t -> Error.t

(** This is the SQLSTATE *)
val postgres_error_code : t -> string option

val postgres_field : t -> Postgres_field.t -> string option
val raise : t -> _
val max_query_length : int ref
val max_parameter_length : int ref
val max_parameters : int ref

val set_error_reporting_limits
  :  ?query_length:int
  -> ?parameter_length:int
  -> ?parameters:int
  -> unit
  -> unit

val tag_by_query : ?parameters:string option array -> query_string:string -> t -> t
