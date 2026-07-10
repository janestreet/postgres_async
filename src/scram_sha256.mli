open! Core

type t

val mechanism : string
val create : ?client_nonce:string -> password:string -> unit -> t
val initial_response : t -> string
val final_response : t -> server_first_message:string -> string Or_error.t
val verify_server_final : t -> server_final_message:string -> unit Or_error.t
