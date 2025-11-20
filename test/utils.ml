open Core
open! Async

(* Most of the error message from postgres is not stable wrt. server version. This is
   probably good enough. *)
let rec delete_unstable_bits_of_error : Sexp.t -> Sexp.t =
  let is_code_pair : Sexp.t -> bool = function
    | List [ Atom "Code"; Atom _ ] -> true
    | _ -> false
  in
  let is_severity_or_code_pair : Sexp.t -> bool = function
    | List [ Atom ("Severity" | "Code"); Atom _ ] -> true
    | _ -> false
  in
  function
  | Atom _ as x -> x
  | List tags when List.exists tags ~f:is_code_pair ->
    List (List.filter tags ~f:is_severity_or_code_pair)
  | List [ (Atom "Writer error from inner_monitor" as e1); e2; _ ] ->
    List [ e1; e2; Atom "<omitted>" ]
  | List list -> List (List.map list ~f:delete_unstable_bits_of_error)
;;

let do_an_epoll =
  lazy
    (let pipe_r, pipe_w = Core_unix.pipe () in
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
;;

let pg_backend_pid postgres =
  let backend_pid = Set_once.create () in
  let%bind result =
    Postgres_async.query
      postgres
      "SELECT pg_backend_pid()"
      ~handle_row:(fun ~column_names:_ ~values ->
        match Iarray.to_array values with
        | [| Some p |] -> Set_once.set_exn backend_pid p
        | _ -> assert false)
  in
  Or_error.ok_exn result;
  return (Set_once.get_exn backend_pid)
;;
