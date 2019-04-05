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
