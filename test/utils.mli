open! Core
open! Async

val delete_unstable_bits_of_error : Sexp.t -> Sexp.t
val do_an_epoll : (unit -> unit Deferred.t) lazy_t
val pg_backend_pid : Postgres_async.t -> string Deferred.t
