open! Core
open! Async
module Protocol = Postgres_async_protocol
module Types = Protocol.Types
module Column_metadata = Protocol.Column_metadata

module type S = sig
  (** In order to provide an [Expert] interface that uses [Pgasync_error.t] to represent
      its errors, alongside a normal interface that just uses Core's [Error.t], the
      interface is defined in this [module type S], with a type [error] that we erase when
      we include it in [postgres_async.mli]. *)
  type error

  type command_complete
  type t [@@deriving sexp_of]

  (** [gss_krb_token] will be sent in response to a server's request to initiate GSSAPI
      authentication. We don't support GSS/SSPI authentication that requires multiple
      steps; if the server sends us a "GSSContinue" message in response to
      [gss_krb_token], login will fail. Kerberos should not require this.

      [ssl_mode] defaults to [Ssl_mode.Disable].

      [buffer_age_limit] sets the age limit on the outgoing Writer.t. The default limit
      is 2m to preserve existing behavior, but you likely want [`Unlimited] to avoid
      application crashes when the database is loaded---and this may become the default at
      some point in the future.

      [buffer_byte_limit] is only used during a COPY---it pauses inserting more rows and
      flushes the entire Writer.t if its buffer contains this many bytes or more. *)
  val connect
    :  ?interrupt:unit Deferred.t
    -> ?ssl_mode:Ssl_mode.t
    -> ?server:_ Tcp.Where_to_connect.t
    -> ?user:string
    -> ?password:string
    -> ?gss_krb_token:string
    -> ?buffer_age_limit:Async_unix.Writer.buffer_age_limit
    -> ?buffer_byte_limit:Byte_units.t
    -> database:string
    -> ?replication:string
    -> unit
    -> (t, error) Result.t Deferred.t

  (** [close] returns an error if there were any problems gracefully tearing down the
      connection. For sure, when it is determined, the connection is gone.

      [try_cancel_statement_before_close] defaults to false. If set to true, [close]
      will first attempt to cancel any query in progress on [t] before closing the
      connection, which provides more 'graceful' closing behavior.
  *)
  val close
    :  ?try_cancel_statement_before_close:bool
    -> t
    -> (unit, error) Result.t Deferred.t

  val close_finished : t -> (unit, error) Result.t Deferred.t

  type state =
    | Open
    | Closing
    | Failed of
        { error : error
        ; resources_released : bool
        }
    | Closed_gracefully
  [@@deriving sexp_of]

  val status : t -> state

  val with_connection
    :  ?interrupt:unit Deferred.t
    -> ?ssl_mode:Ssl_mode.t
    -> ?server:_ Tcp.Where_to_connect.t
    -> ?user:string
    -> ?password:string
    -> ?gss_krb_token:string
    -> ?buffer_age_limit:Async_unix.Writer.buffer_age_limit
    -> ?buffer_byte_limit:Byte_units.t
    -> ?try_cancel_statement_before_close:bool
    -> database:string
    -> ?replication:string
    -> on_handler_exception:[ `Raise ]
    -> (t -> 'res Deferred.t)
    -> ('res, error) Result.t Deferred.t

  (** [handle_columns] can provide column information even if 0 rows are found.
      [handle_columns] is guaranteed to be called before the first invocation of
      [handle_row] *)
  type 'handle_row with_query_args :=
    t
    -> ?parameters:string option array
    -> ?pushback:(unit -> unit Deferred.t)
    -> ?handle_columns:(Column_metadata.t array -> unit)
    -> string
    -> handle_row:'handle_row
    -> (command_complete, error) Result.t Deferred.t

  val query
    : (column_names:string array -> values:string option array -> unit) with_query_args

  val query_zero_copy : (Row_handle.t -> unit) with_query_args

  val query_expect_no_data
    :  t
    -> ?parameters:string option array
    -> string
    -> (command_complete, error) Result.t Deferred.t

  type 'a feed_data_result =
    | Abort of { reason : string }
    | Wait of unit Deferred.t
    | Data of 'a
    | Finished

  val copy_in_raw
    :  t
    -> ?parameters:string option array
    -> string
    -> feed_data:(unit -> string feed_data_result)
    -> (command_complete, error) Result.t Deferred.t

  (** Note that [table_name] and [column_names] must be escaped before calling
      [copy_in_rows]. *)
  val copy_in_rows
    :  ?schema_name:string
    -> t
    -> table_name:string
    -> column_names:string list
    -> feed_data:(unit -> string option array feed_data_result)
    -> (command_complete, error) Result.t Deferred.t

  (** [listen_to_notifications] executes a query to subscribe you to notifications on
      [channel] (i.e., "LISTEN $channel") and stores [f] inside [t], calling it when the
      server sends us any such notifications.

      Calling it multiple times is fine: the "LISTEN" query is idempotent, and both
      callbacks will be stored in [t].

      However, be careful. The interaction between postgres notifications and transactions
      is very subtle. Here are but some of the things you need to bear in mind:

      - LISTEN executed during a transaction that is rolled back has no effect. As such,
        if you're in the middle of a transaction when you call [listen_to_notifications]
        and then roll said transaction back, [f] will be stored in [t] but you will not
        actually receive any notifications from the server.

      - Notifications that happen while you are in a transaction are only delivered by the
        server after the end of the transaction. In particular, if you're doing a big
        [query] and you're pushing-back on the server, you're also potentially delaying
        delivery of notifications.

      - You need to pay attention to [close_finished] in case the server kicks you off.

      - The postgres protocol has no heartbeats. If the server disappears in
        a particularly bad way it might be a while before we notice. The empty query makes
        a rather effective heartbeat (i.e. [query_expect_no_data t ""]), but this is your
        responsibility if you want it. *)
  val listen_to_notifications
    :  t
    -> channel:string
    -> f:(pid:Pid.t -> payload:string -> unit)
    -> (command_complete, error) Result.t Deferred.t
end

module type Pgasync_error = sig
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
  type t = Pgasync_error.t [@@deriving sexp_of]

  module Postgres_field = Pgasync_error.Postgres_field

  val to_error : t -> Error.t
  val postgres_error_code : t -> string option
  val raise : t -> 'a
  val postgres_field : t -> Postgres_field.t -> string option

  (** Queries and query parameters are included in the errors. To keep the length of error
      messages in check, this information is abbreviated:
      - long queries are truncated
      - long parameter values are truncated
      - only a limited number of parameters are displayed

      You can change those global limits via [set_error_reporting_limits]
    *)
  val set_error_reporting_limits
    :  ?query_length:int (** default: 2048 *)
    -> ?parameter_length:int (** default: 128 *)
    -> ?parameters:int (** default: 16 *)
    -> unit
    -> unit
end

module type Private = sig
  type t

  module Protocol = Protocol
  module Types = Types
  module Pgasync_error = Pgasync_error
  module Row_handle = Row_handle

  val pgasync_error_of_error : Error.t -> Pgasync_error.t
  val pq_cancel : t -> unit Or_error.t Deferred.t

  module Simple_query_result : sig
    (**
       - Completed_with_no_warnings : everything worked as expected. The list will contain
         one Command_complete.t for each statement executed
       - Completed_with_warnings : the query ran successfully, but the query tried to do
         something unsupported client-side (e.g. COPY TO STDOUT). The list of errors
         will not be empty.
       - Connection_error : The underlying connection died at some point during query
         execution or parsing results. Query may or may not have taken effect on server.
       - Driver_error : Postgres_async received an unexpected protocol message from the
         server. Query may or may not have taken effect on server.
       - Failed : Got error from server, query did not take effect on server
    *)

    type t =
      | Completed_with_no_warnings of Command_complete.t list
      | Completed_with_warnings of (Command_complete.t list * Error.t list)
      | Failed of Pgasync_error.t
      | Connection_error of Pgasync_error.t
      | Driver_error of Pgasync_error.t

    val to_or_pgasync_error : t -> Command_complete.t list Or_pgasync_error.t
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

  module Message_reading : Message_reading_intf.S with type t := t
end

module type Postgres_async = sig
  module Pgasync_error : Pgasync_error

  module Or_pgasync_error : sig
    type 'a t = ('a, Pgasync_error.t) Result.t [@@deriving sexp_of]

    val to_or_error : 'a t -> 'a Or_error.t
    val ok_exn : 'a t -> 'a
  end

  module Column_metadata : Column_metadata.Public
  module Command_complete : Command_complete.Public
  module Row_handle = Row_handle
  module Ssl_mode = Ssl_mode

  type t [@@deriving sexp_of]

  module type S := S with type t := t

  (** @open *)
  include S with type error := Error.t and type command_complete := unit

  (** The [Expert] module provides versions of all the same functions that instead return
    [Or_pgasync_error.t]s.

    Note that [t] and [Expert.t] is the same type, so you can mix-and-match depending on
    whether you want to try and inspect the error code of a specific failure or not. *)
  module Expert : S with type error := Pgasync_error.t and type command_complete := unit

  (** The [With_command_complete] module provides access to the contents of
      CommandComplete message, that includes a string tag that describes the command that
      was executed, and optional row count *)
  module With_command_complete :
    S with type error := Error.t and type command_complete := Command_complete.t

  module Expert_with_command_complete :
    S with type error := Pgasync_error.t and type command_complete := Command_complete.t

  module Private : Private with type t := t
end
