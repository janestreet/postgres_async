open Core
open! Async

(* Most of the error message from postgres is not stable wrt. server version.
   This is probably good enough. *)
let rec delete_unstable_bit_of_server_error : Sexp.t -> Sexp.t =
  let is_severity_pair : Sexp.t -> bool =
    function
    | List [Atom "severity"; Atom _] -> true
    | _ -> false
  in
  let is_severity_or_code_pair : Sexp.t -> bool =
    function
    | List [Atom ("severity" | "code"); Atom _] -> true
    | _ -> false
  in
  function
  | Atom _ as x -> x
  | List tags when List.exists tags ~f:is_severity_pair ->
    List (List.filter tags ~f:is_severity_or_code_pair)
  | List list ->
    List (List.map list ~f:delete_unstable_bit_of_server_error)

let do_an_epoll =
  lazy (
    let (pipe_r, pipe_w) = Core.Unix.pipe () in
    let pipe_r = Fd.create Char pipe_r (Info.of_string "do-an-epoll-pipe-r") in
    let pipe_w = Fd.create Char pipe_w (Info.of_string "do-an-epoll-pipe-w") in
    let reader = Reader.create pipe_r in
    let writer = Writer.create pipe_w in
    fun () ->
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      Writer.write_char writer 'x';
      let%bind () =
        match%bind Reader.read_char reader with
        | `Ok 'x' -> return ()
        | _ -> assert false
      in
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      return ())
