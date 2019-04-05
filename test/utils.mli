open! Core
open! Async

val delete_unstable_bit_of_server_error : Sexp.t -> Sexp.t

val do_an_epoll : (unit -> unit Deferred.t) lazy_t
