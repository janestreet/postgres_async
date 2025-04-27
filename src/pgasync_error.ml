open Core
module ErrorResponse = Postgres_async_protocol.Backend.ErrorResponse
module Postgres_field = Postgres_async_protocol.Backend.Error_or_notice_field

(* The MLI introduces the [Pgasync_error] type; it's our place to store the generic
     error, and the error code _if_ we know it.

     Now, on the subject of error handling more generally, there are a few axes on which
     one must make a decision here.

     API
     ---
     First and foremost, we need to decide whether or not we should expose a single api,
     where all the result types are [Or_pgasync_error.t], or should we expose a more
     convenient API that uses [Or_error.t], and an expert API that uses
     [Or_pgasync_error.t]? We do the latter, because the vast majority of our users don't
     care for the [postgres_error_code] and do use [Error.t] everywhere else.

     Internal storage of connection-killing errors
     ---------------------------------------------
     But more subtly, once you've agreed to provide [postgres_error_code], you need to be
     very careful about when you provide it. If you return an error from [query] with
     [postgres_error_code = Some _] in it, then the user will quite reasonably assume that
     the query failed with that error. But that might not be true: if the connection
     previously asynchronously failed due to some error (say, the connection closed),
     we're going to return that error to the user.

     This is a problem, because the error that closed the connection might imply that
     a specific thing is wrong with their query, when actually it is not. I don't have
     a great example, but suppose that there existed an error code relating to replication
     that might cause the backend to die.

     If the backend asynchronously dies, we'll close [t] and stash the reason it closed
     inside [t.state]. If someone then tries to use [query t], we'll fetch the reason
     out of [t.state], and return that as an error. But if we did this naively, then it
     would look to the user like their specific query conflicted with replication, and
     they might retry it, when actually that was unrelated.

     The important thing here is: we should only return a [postgres_error_code] that has
     semantic meaning to the user _if_ it relates to the operation they just performed,
     not some operation that happened previously.

     You could imagine two different ways of trying to achieve this:

     + Only ever stash [Error.t]s inside [state] (or similar). This ensures we'll never
     fetch a [postgres_error_code] out of storage and give it to the user.

     But this is tricky, because now we have a mixture of different error types in the
     library, which is super annoying and messy, and you have to be careful about when you
     use one vs. the other, and you have to keep converting back and forth.

     Furthermore, you need to be careful that a refactor cause an error relating to
     a specific query to be stashed and immediately retrieved, as that might erase the
     error code, or passed through some generic function that erases

     + Use [Pgasync_error.t] everywhere within the library, but before every operation
     like [query], examine [t.state] to see if the connection is already dead, and make
     sure that when you return the stashed [Pgasync_error.t], you erase the code from it
     (and tag it with a message like "query issued on closed connection; original error
     was foo" too).

     We go with the latter.

     This is precisely achieved by the first line of [parse_and_start_executing_query].

     We're using the simplifying argument that even if an error gets stashed in [t.state]
     and then returned to the user at the end of [query], that error _wasn't_ there when
     the query was started, so it's reasonable to associate it with the [query], which
     sounds fine. *)

module Sqlstate = struct
  type t = string [@@deriving compare, equal, hash, sexp_of]

  let cardinality_violation = "21000"
  let invalid_authorization_specification = "28000"
  let invalid_password = "28P01"
  let connection_exception = "08000"
  let sqlclient_unable_to_establish_sqlconnection = "08001"
  let connection_does_not_exist = "08003"
  let sqlserver_rejected_establishment_of_sqlconnection = "08004"
  let connection_failure = "08006"
  let protocol_violation = "08P01"
  let object_not_in_prerequisite_state = "55000"
  let undefined_object = "42704"
  let wrong_object_type = "42809"
  let syntax_error = "42601"
end

type t =
  { error : Error.t
  ; server_error : ErrorResponse.t option
  }

let sexp_of_t t = [%sexp (t.error : Error.t)]

let of_error ?error_code e =
  { error = e
  ; server_error =
      Option.map error_code ~f:(fun error_code : ErrorResponse.t ->
        { error_code; all_fields = [] })
  }
;;

let of_exn ?error_code e = of_error ?error_code (Error.of_exn e)
let of_string ?error_code s = of_error ?error_code (Error.of_string s)
let create_s ?error_code s = of_error ?error_code (Error.create_s s)

let of_error_response (error_response : ErrorResponse.t) =
  let error =
    (* We omit some of the particularly noisy and uninteresting fields from the error
         message that will be displayed to users.

         Note that as-per [ErrorResponse.t]'s docstring, [Code] is included in this
         list. *)
    let interesting_fields =
      List.filter error_response.all_fields ~f:(fun (field, value) ->
        match field with
        | File | Line | Routine | Severity_non_localised -> false
        | Severity ->
          (* ERROR is the normal case for an error message, so just omit it *)
          String.( <> ) value "ERROR"
        | _ -> true)
    in
    Error.create_s [%sexp (interesting_fields : (Postgres_field.t * string) list)]
  in
  { error; server_error = Some error_response }
;;

let tag t ~tag = { t with error = Error.tag t.error ~tag }
let to_error t = t.error

let postgres_error_code t =
  match t.server_error with
  | None -> None
  | Some { error_code; _ } -> Some error_code
;;

let postgres_field t field =
  match t.server_error with
  | None -> None
  | Some { all_fields; _ } ->
    List.Assoc.find all_fields field ~equal:[%equal: Postgres_field.t]
;;

let raise t = Error.raise t.error

(* Truncate queries longer than this *)
let max_query_length = ref 2048
let max_parameter_length = ref 128
let max_parameters = ref 16

let set_error_reporting_limits
  ?(query_length = 2048)
  ?(parameter_length = 128)
  ?(parameters = 16)
  ()
  =
  max_query_length := query_length;
  max_parameter_length := parameter_length;
  max_parameters := parameters
;;

let query_tag ?parameters ~query_string t =
  lazy
    (let position =
       (* Postgres reports 1-based position *)
       Option.value_map ~default:1 ~f:Int.of_string (postgres_field t Position) - 1
     in
     (* We want to display the long query like this:

            <prefix>[error position]<suffix>

            where combined length of lead + prefix + suffix is less than
            max_query_length *)
     let half = !max_query_length / 2 in
     let prefix_start = max 0 (position - half) in
     let query_length = String.length query_string in
     let suffix_end = min query_length (position + half) in
     let prefix =
       (* end position 0 means whole string, we dont want this *)
       if position = 0 then "" else String.slice query_string prefix_start position
     in
     let suffix = String.slice query_string position suffix_end in
     let query =
       String.concat
         ~sep:""
         [ (if prefix_start > 0 then "... " else "")
         ; prefix
         ; suffix
         ; (if suffix_end < query_length then " ..." else "")
         ]
     in
     let parameters =
       (* Limit the number of parameters *)
       Option.bind parameters ~f:(fun parameters ->
         if Array.is_empty parameters
         then None
         else if Array.length parameters > !max_parameters
         then
           Some
             (Array.init !max_parameters ~f:(fun idx ->
                if idx = !max_parameters - 1
                then (
                  let num_omitted = Array.length parameters - !max_parameters in
                  Some [%string "remaining %{num_omitted#Int} parameter(s) omitted"])
                else parameters.(idx)))
         else Some parameters)
     in
     let parameters =
       (* Limit the length of parameters *)
       Option.map parameters ~f:(fun parameters ->
         Array.map
           parameters
           ~f:
             (Option.map ~f:(fun param ->
                if String.length param > !max_parameter_length
                then String.prefix param !max_parameter_length ^ "..."
                else param)))
     in
     [%message (query : string) (parameters : (string option array option[@sexp.option]))])
;;

let tag_by_query ?parameters ~query_string t =
  { t with error = Error.tag_s_lazy t.error ~tag:(query_tag ?parameters ~query_string t) }
;;
