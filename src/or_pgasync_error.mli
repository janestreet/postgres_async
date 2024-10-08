open Core

type 'a t = ('a, Pgasync_error.t) Result.t [@@deriving sexp_of]

val to_or_error : 'a t -> 'a Or_error.t
val ok_exn : 'a t -> 'a
val error_s : ?error_code:Pgasync_error.Sqlstate.t -> Sexp.t -> _ t
val error_string : ?error_code:Pgasync_error.Sqlstate.t -> string -> _ t
val of_exn : ?error_code:Pgasync_error.Sqlstate.t -> Exn.t -> _ t

val errorf
  :  ?error_code:Pgasync_error.Sqlstate.t
  -> ('a, unit, string, 'b t) format4
  -> 'a
