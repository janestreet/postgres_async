open! Core
open Async

(** This module is like [Sequencer], except it provides the [when_idle] function.

    [when_idle] allows you to perform some task while the sequencer is empty (i.e., no job
    running and nothing waiting to start). Your callback is called repeatedly while the
    queue is empty.

    Jobs will not be permitted to start until the [when_idle] callback returns, so that
    they may both use some shared resource without stepping on each other. Your callback
    should use [other_jobs_are_waiting] to know when is the right time to interrupt doing
    whatever it is doing and return.

    If it returns [Call_me_when_idle_again] early (i.e., before [other_jobs_are_waiting]
    is determined), then it will be immediately called again.

    If it returns [Finished], the when-idle callback will be deleted from [t].

    Unlike [Sequencer], this module does nothing smart with exceptions; we provide no
    promises as to which monitor they will go to. This is because [Postgres_async] does
    not use/raise exceptions (bugs aside). *)

type t [@@deriving sexp_of]

val create : unit -> t
val enqueue : t -> (unit -> 'a Deferred.t) -> 'a Deferred.t

type when_idle_next_step =
  | Call_me_when_idle_again
  | Finished

(** At most one 'when_idle' can be active at once; the previous callback must have
    returned [Finished] before a new one can be installed. *)
val when_idle : t -> (unit -> when_idle_next_step Deferred.t) -> unit

val other_jobs_are_waiting : t -> unit Deferred.t
