open! Core

type t

val create
  :  ?extra_server_args:string list
  -> unit
  -> t

val create_database : t -> string -> unit

val pg_hba_filename : t -> string
val pg_ident_filename : t -> string

val sighup_server : t -> unit

val unix_socket_path : t -> string
val port : t -> int

open! Async

val where_to_connect : t -> Socket.Address.Unix.t Tcp.Where_to_connect.t

val with_connection_exn
  :  t
  -> ?user:string (* default: the super user, postgres. *)
  -> database:string
  -> (Postgres_async.t -> unit Deferred.t)
  -> unit Deferred.t
