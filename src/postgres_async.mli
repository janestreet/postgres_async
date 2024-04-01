open Core
open! Async

module Pgasync_error : sig
  (** The type [Pgasync_error.t] is used for all errors in this library.

      [to_error] returns a regular [Core.Error.t] that fully describes what went wrong
      (including but not limited to a postgres error code, if we hae one), and if you only
      need to show the error to someone/raise/fail then using that completely suffices.

      _If_ the error due to an [ErrorResponse] from the server _and_ we successfully
      parsed an error code out of it, you can retrieve that via [postgres_error_code].

      Note that either of those conditions might be false, e.g. the error might be due to
      TCP connection failure, so we certainly won't have any postgres error code; you
      shouldn't take the name of the type to mean "postgres error", rather "postgres async
      error"---any error this library can produce. *)
  type t [@@deriving sexp_of]

  val to_error : t -> Error.t
  val postgres_error_code : t -> string option
  val raise : t -> 'a

  module Postgres_field : sig
    type other = private char

    type t =
      | Severity
      | Severity_non_localised
      | Code
      | Message
      | Detail
      | Hint
      | Position
      | Internal_position
      | Internal_query
      | Where
      | Schema
      | Table
      | Column
      | Data_type
      | Constraint
      | File
      | Line
      | Routine
      | Other of other
    [@@deriving sexp_of]
  end

  val postgres_field : t -> Postgres_field.t -> string option
end

module Or_pgasync_error : sig
  type 'a t = ('a, Pgasync_error.t) Result.t [@@deriving sexp_of]

  val to_or_error : 'a t -> 'a Or_error.t
  val ok_exn : 'a t -> 'a
end

module Column_metadata : sig
  (** Contains information on the name and type of a column in query results *)
  type t

  val name : t -> string

  (** Oid of the type of data in the column. To get full type information for
      some [pg_type_oid t = K], [select * from pg_type where oid = K]. *)
  val pg_type_oid : t -> int
end

module Ssl_mode : sig
  (** [t] is a subset of the types supported by the 'sslmode' parameter in libpq
      (documented at https://www.postgresql.org/docs/current/libpq-ssl.html).

      We don't currently support verifying certificate signatures, so there's nothing
      analogous to the "verify-ca" or "verify-full" options here. We don't distinguish
      between "allow" and "prefer" (they seem to exactly match in terms of behavior).

      Under the hood, [Prefer] and [Require] will both result in the very first message
      sent to the server being the [SSLRequest] message, instead of the [StartupMessage].
      The server will respond with whether or not it is able to support SSL.

      Using [Require], [connect] will return an error if the server cannot support SSL.

      [Prefer] will SSL-wrap the connection if the server supports SSL, or will use
      a plain TCP connection if the server does not support SSL.

      [Disable] will always use the plain TCP connection, and will not send the
      [SSLRequest] message. *)
  type t =
    | Disable
    | Prefer
    | Require

  val to_libpq_string : t -> string
  val of_libpq_string : string -> t option
end

type t [@@deriving sexp_of]

include
  Postgres_async_intf.S
    with type t := t
     and type error := Error.t
     and type column_metadata := Column_metadata.t
     and type ssl_mode := Ssl_mode.t

(** The [Expert] module provides versions of all the same functions that instead return
    [Or_pgasync_error.t]s.

    Note that [t] and [Expert.t] is the same type, so you can mix-and-match depending on
    whether you want to try and inspect the error code of a specific failure or not. *)
module Expert :
  Postgres_async_intf.S
    with type t := t
     and type error := Pgasync_error.t
     and type column_metadata := Column_metadata.t
     and type ssl_mode := Ssl_mode.t

module Private : sig
  module Protocol = Protocol
  module Types = Types

  val pgasync_error_of_error : Error.t -> Pgasync_error.t
  val pq_cancel : t -> unit Or_error.t Deferred.t

  module Simple_query_result : sig
    (**
       - Completed_with_no_warnings : everything worked as expected
       - Completed_with_warnings : the query ran successfully, but the query tried to do
         something unsupported client-side (e.g. COPY TO STDOUT).
       - Connection_error : The underlying connection died at some point during query
         execution or parsing results. Query may or may not have taken effect on server.
       - Driver_error : Postgres_async received an unexpected protocol message from the
         server. Query may or may not have taken effect on server.
       - Failed : Got error from server, query did not take effect on server
    *)

    type t =
      | Completed_with_no_warnings
      | Completed_with_warnings of Error.t list
      | Failed of Pgasync_error.t
      | Connection_error of Pgasync_error.t
      | Driver_error of Pgasync_error.t

    val to_or_pgasync_error : t -> unit Or_pgasync_error.t
  end

  (** Executes a query according to the Postgres Simple Query protocol.  As specified in
      the protocol, multiple commands can be chained together using semicolons and
      executed in one operation. If not already in a transaction, the query is treated as
      transaction and all commands within it are executed atomically.

      [handle_columns] is called on each column row description message. Note that
      with simple_query, it's possible that multiple row description messages are
      sent (as a simple query can contain multiple statements that return rows).

      [handle_row] is called for every row returned by the query.

      If a [Pgasync_error.t] is returned, this indicates that the connection was closed
      during processing. The query may or may not have successfully ran from the server's
      perspective.


      Queries containing COPY FROM STDIN will fail as this function does not support this
      operation.  Queries containing COPY TO STDOUT will succeed, but the copydata will
      not be delivered to [handle_row], and a warning will appear in [Simple_query_status]
  *)
  val simple_query
    :  ?handle_columns:(Column_metadata.t array -> unit)
    -> t
    -> string
    -> handle_row:(column_names:string array -> values:string option array -> unit)
    -> Simple_query_result.t Deferred.t

  (** Executes a query that should not return any rows using the Postgres Simple Query
      Protocol . As with [simple_query], multiple commands can be chained together using
      semicolons. Inherits the transaction behavior of [simple_query].

      If any of the queries fails, or returns at least one row, an error will be returned
      and the transaction will be aborted.

      As with [simple_query], queries containing COPY FROM STDIN will fail.*)
  val execute_simple : t -> string -> Simple_query_result.t Deferred.t

  module Without_background_asynchronous_message_handling : sig
    type t

    (** Creates a TCP connection to the server, returning the Reader and
        Writer after the login message flow has been completed successfully.
        Will not have asynchronous message handling running *)
    val login_and_get_raw
      :  ?interrupt:unit Deferred.t
      -> ?ssl_mode:Ssl_mode.t
      -> ?server:[< Socket.Address.t ] Tcp.Where_to_connect.t
      -> ?password:string
      -> ?gss_krb_token:string
      -> ?buffer_age_limit:[ `At_most of Core_private.Span_float.t | `Unlimited ]
      -> ?buffer_byte_limit:Byte_units.t
      -> startup_message:Protocol.Frontend.StartupMessage.t
      -> unit
      -> (t, Pgasync_error.t) result Deferred.t

    val reader : t -> Reader.t
    val writer : t -> Writer.t
    val backend_key : t -> Types.backend_key option
    val runtime_parameters : t -> string String.Map.t
    val pq_cancel : t -> unit Deferred.Or_error.t
  end
end
