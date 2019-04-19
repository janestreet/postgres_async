open Core
open Async

type t [@@deriving sexp_of]

(** [gss_krb_token] will be sent in response to a server's request to initiate GSSAPI
    authentication. We don't support GSS/SSPI authentication that requires multiple steps;
    if the server sends us a "GSSContinue" message in response to [gss_krb_token], login
    will fail. Kerberos should not require this. *)
val connect
  :  ?interrupt:unit Deferred.t
  -> ?server:_ Tcp.Where_to_connect.t
  -> ?user:string
  -> ?password:string
  -> ?gss_krb_token:string
  -> database:string
  -> unit
  -> t Or_error.t Deferred.t

(** [close] returns an error if there were any problems gracefully tearing down
    the connection. For sure, when it is determined, the connection is gone. *)
val close : t -> unit Or_error.t Deferred.t

val close_finished : t -> unit Or_error.t Deferred.t

val with_connection
  :  ?interrupt:unit Deferred.t
  -> ?server:_ Tcp.Where_to_connect.t
  -> ?user:string
  -> ?password:string
  -> ?gss_krb_token:string
  -> database:string
  -> (t -> 'res Deferred.t)
  -> 'res Or_error.t Deferred.t

val query
  :  t
  -> ?parameters:string option array
  -> ?pushback:(unit -> unit Deferred.t)
  -> string
  -> handle_row:(column_names:string array -> values:string option array -> unit)
  -> unit Or_error.t Deferred.t

val query_expect_no_data
  :  t
  -> ?parameters:string option array
  -> string
  -> unit Or_error.t Deferred.t

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
  -> unit Or_error.t Deferred.t

val copy_in_rows
  :  t
  -> table_name:string
  -> column_names:string array
  -> feed_data:(unit -> string option array feed_data_result)
  -> unit Or_error.t Deferred.t

(** [listen_to_notifications] executes a query to subscribe you to notifications on
    [channel] (i.e., "LISTEN $channel") and stores [f] inside [t], calling it when the
    server sends us any such notifications.

    Calling it multiple times is fine: the "LISTEN" query is idempotent, and both
    callbacks will be stored in [t].

    However, be careful. The interaction between postgres notifications and transactions
    is very subtle. Here are but some of the things you need to bear in mind:

    - LISTEN executed during a transaction that is rolled back has no effect. As such, if
      you're in the middle of a transaction when you call [listen_to_notifications] and
      then roll said transaction back, [f] will be stored in [t] but you will not actually
      receive any notifications from the server.

    - Notifications that happen while you are in a transaction are only delivered by the
      server after the end of the transaction. In particular, if you're doing a big
      [query] and you're pushing-back on the server, you're also potentially delaying
      delivery of notifications.

    - You need to pay attention to [close_finished] in case the server kicks you off.

    - The postgres protocol has no heartbeats. If the server disappears in a particularly
      bad way it might be a while before we notice. The empty query makes a rather
      effective heartbeat (i.e. [query_expect_no_data t ""]), but this is your
      responsibility if you want it. *)
val listen_to_notifications
  :  t
  -> channel:string
  -> f:(payload:string -> unit)
  -> unit Or_error.t Deferred.t
