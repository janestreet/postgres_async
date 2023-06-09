open Core
open Async
open! Int.Replace_polymorphic_compare

(* Why don't we implement this using [Sequencer] (and thereby get its nice exception
   handling, notably)?

   Well, you end up writing some loop like this

   {[
     let rec run t =
       match Throttle.num_jobs_running s with
       | 0 -> when_idle etc.
       | _ ->
         (* wait for job to finish. *)
         run t
   ]}

   the problem is that "wait for job to finish" is hard. You can enqueue a job in the
   sequencer, or use [Throttle.prior_jobs_done], but both of those things modify
   [Throttle.num_jobs_running] and there are no guarantees about the race between
   [num_jobs_running] changing back to [0] and the result of the dummy job you enqueued
   becoming determined. In practice async-callbacks waiting on dummy job's deferred run
   before [num_jobs_running] decreases, and you end up in an infinite loop.

   We could probably hack around that with [Scheduler.yield_until_no_jobs_remain ()] or
   something but besides being slow, I claim this ultimately ends up being far more
   complicated than that which you see below. *)

type job =
  | Job :
      { start : unit Ivar.t
      ; finished : 'a Deferred.t
      }
      -> job
[@@deriving sexp_of]

type when_idle_next_step =
  | Call_me_when_idle_again
  | Finished

type t =
  { jobs_waiting : job Queue.t
  ; any_work_added : (unit, read_write) Bvar.t
  ; mutable when_idle : (unit -> when_idle_next_step Deferred.t) option
  }
[@@deriving sexp_of]

let rec run t =
  let%bind () =
    match Queue.dequeue t.jobs_waiting with
    | Some (Job { start; finished }) ->
      Ivar.fill_exn start ();
      let%bind _ = finished in
      return ()
    | None ->
      (match t.when_idle with
       | None -> Bvar.wait t.any_work_added
       | Some func ->
         (match%bind func () with
          | Finished ->
            t.when_idle <- None;
            return ()
          | Call_me_when_idle_again -> return ()))
  in
  run t
;;

let create () =
  let t =
    { jobs_waiting = Queue.create (); any_work_added = Bvar.create (); when_idle = None }
  in
  don't_wait_for (run t);
  t
;;

let enqueue t job : _ Deferred.t =
  let start = Ivar.create () in
  let finished =
    let%bind () = Ivar.read start in
    job ()
  in
  Queue.enqueue t.jobs_waiting (Job { start; finished });
  Bvar.broadcast t.any_work_added ();
  finished
;;

let when_idle t callback =
  match t.when_idle with
  | Some _ -> failwith "Query_scheduler.when_idle: already have a callback"
  | None ->
    t.when_idle <- Some callback;
    Bvar.broadcast t.any_work_added ()
;;

let rec other_jobs_are_waiting t =
  match Queue.is_empty t.jobs_waiting with
  | false -> return ()
  | true ->
    let%bind () = Bvar.wait t.any_work_added in
    other_jobs_are_waiting t
;;
