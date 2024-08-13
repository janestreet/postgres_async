open! Core
open Async

module type S = sig
  type column_metadata
  type ssl_mode
  type command_complete

  (** In order to provide an [Expert] interface that uses [Pgasync_error.t] to represent
      its errors, alongside a normal interface that just uses Core's [Error.t], the
      interface is defined in this [module type S], with a type [error] that we erase when
      we include it in [postgres_async.mli]. *)
  type error

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
    -> ?ssl_mode:ssl_mode
    -> ?server:_ Tcp.Where_to_connect.t
    -> ?user:string
    -> ?password:string
    -> ?gss_krb_token:string
    -> ?buffer_age_limit:Async_unix.Writer.buffer_age_limit
    -> ?buffer_byte_limit:Byte_units.t
    -> database:string
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
    -> ?ssl_mode:ssl_mode
    -> ?server:_ Tcp.Where_to_connect.t
    -> ?user:string
    -> ?password:string
    -> ?gss_krb_token:string
    -> ?buffer_age_limit:Async_unix.Writer.buffer_age_limit
    -> ?buffer_byte_limit:Byte_units.t
    -> ?try_cancel_statement_before_close:bool
    -> database:string
    -> on_handler_exception:[ `Raise ]
    -> (t -> 'res Deferred.t)
    -> ('res, error) Result.t Deferred.t

  (** [handle_columns] can provide column information even if 0 rows are found.
      [handle_columns] is guaranteed to be called before the first invocation of
      [handle_row] *)
  val query
    :  t
    -> ?parameters:string option array
    -> ?pushback:(unit -> unit Deferred.t)
    -> ?handle_columns:(column_metadata array -> unit)
    -> string
    -> handle_row:(column_names:string array -> values:string option array -> unit)
    -> (command_complete, error) Result.t Deferred.t

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
