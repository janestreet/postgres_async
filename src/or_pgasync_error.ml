open Core

type 'a t = ('a, Pgasync_error.t) Result.t [@@deriving sexp_of]

let to_or_error = Result.map_error ~f:Pgasync_error.to_error

let ok_exn = function
  | Ok x -> x
  | Error e -> Pgasync_error.raise e
;;

let error_s ?error_code s = Error (Pgasync_error.create_s ?error_code s)
let error_string ?error_code s : _ t = Error (Pgasync_error.of_string ?error_code s)
let errorf ?error_code fmt = ksprintf (error_string ?error_code) fmt
let of_exn ?error_code e = Error (Pgasync_error.of_exn ?error_code e)
