open! Core
module Cryptokit = Crypto.Cryptokit

(* SCRAM-SHA-256 authentication, as described in
   https://datatracker.ietf.org/doc/html/rfc7677 *)

let mechanism = "SCRAM-SHA-256"
let client_key = "Client Key"
let server_key = "Server Key"
let gs2_header = "n,,"
let gs2_header_base64 = Base64.encode_exn gs2_header
let key_length = 32
let hmac key data = Cryptokit.(hash_string (MAC.hmac_sha256 key) data)
let sha256 data = Cryptokit.hash_string (Cryptokit.Hash.sha256 ()) data

module Attribute = struct
  let random_nonce = 'r'
  let base64_salt = 's'
  let iteration_count = 'i'
end

let pbkdf2_sha256 ~password ~salt ~iterations =
  Pbkdf.pbkdf2
    ~prf:`SHA256
    ~password:(Cstruct.of_string password)
    ~salt:(Cstruct.of_string salt)
    ~count:iterations
    ~dk_len:(Int32.of_int_exn key_length)
  |> Cstruct.to_string
;;

let xor_strings_exn a b =
  if String.length a <> String.length b
  then (
    let a_length = String.length a in
    let b_length = String.length b in
    raise_s
      [%message
        "cannot xor strings with different lengths" (a_length : int) (b_length : int)]);
  String.mapi a ~f:(fun i char ->
    Char.of_int_exn (Char.to_int char lxor Char.to_int b.[i]))
;;

let random_nonce () =
  Cryptokit.Random.string Cryptokit.Random.secure_rng 18 |> Base64.encode_string
;;

type t =
  { password : string
  ; client_nonce : string
  ; client_first_message_bare : string
  ; mutable server_first_message : string option
  ; mutable client_final_message_without_proof : string option
  ; mutable salted_password : string option
  }

let create ?client_nonce ~password () =
  let client_nonce = Option.value client_nonce ~default:(random_nonce ()) in
  { password
  ; client_nonce
  ; client_first_message_bare = [%string "n=,r=%{client_nonce}"]
  ; server_first_message = None
  ; client_final_message_without_proof = None
  ; salted_password = None
  }
;;

let initial_response t = gs2_header ^ t.client_first_message_bare

let parse_attributes message ~expected_attributes =
  let expected_attributes = Char.Set.of_list expected_attributes in
  String.split message ~on:','
  |> List.fold_result ~init:Char.Map.empty ~f:(fun attributes field ->
    match String.lsplit2 field ~on:'=' with
    | None -> Or_error.error_s [%message "SCRAM attribute does not contain '='" field]
    | Some (attribute, value) ->
      (match String.to_list attribute with
       | [ attribute ] ->
         if not (Set.mem expected_attributes attribute)
         then
           Or_error.error_s
             [%message "unexpected SCRAM attribute" (attribute : char) message]
         else (
           match Map.add attributes ~key:attribute ~data:value with
           | `Ok attributes -> Ok attributes
           | `Duplicate ->
             Or_error.error_s
               [%message "duplicate SCRAM attribute" (attribute : char) message])
       | _ -> Or_error.error_s [%message "malformed SCRAM attribute" attribute message]))
;;

let find_attribute attributes attribute =
  match Map.find attributes attribute with
  | Some value -> Ok value
  | None -> Error (Error.create_s [%message "missing SCRAM attribute" (attribute : char)])
;;

let final_response t ~server_first_message =
  let%bind.Or_error attributes =
    parse_attributes
      server_first_message
      ~expected_attributes:Attribute.[ random_nonce; base64_salt; iteration_count ]
  in
  let%bind.Or_error nonce = find_attribute attributes Attribute.random_nonce in
  if not (String.is_prefix nonce ~prefix:t.client_nonce)
  then (
    let client_nonce = t.client_nonce in
    let server_nonce = nonce in
    Or_error.error_s
      [%message
        "SCRAM server nonce does not start with client nonce"
          (client_nonce : string)
          (server_nonce : string)])
  else (
    let%bind.Or_error encoded_salt = find_attribute attributes 's' in
    let%bind.Or_error salt =
      Or_error.try_with (fun () -> Base64.decode_exn encoded_salt)
      |> Result.map_error ~f:(fun error ->
        Error.tag_arg error "malformed SCRAM salt" encoded_salt [%sexp_of: string])
    in
    let%bind.Or_error iterations =
      let%bind.Or_error string = find_attribute attributes Attribute.iteration_count in
      match Int.of_string string with
      | exception exn ->
        Or_error.error_s
          [%message
            "malformed SCRAM integer attribute"
              ~attribute:(Attribute.iteration_count : char)
              (string : string)
              (exn : Exn.t)]
      | int when int < 1 ->
        Or_error.error_s
          [%message
            "SCRAM integer attribute must be positive"
              ~attribute:(Attribute.iteration_count : char)
              (int : int)]
      | int -> Ok int
    in
    let client_final_message_without_proof =
      [%string "c=%{gs2_header_base64},r=%{nonce}"]
    in
    let salted_password = pbkdf2_sha256 ~password:t.password ~salt ~iterations in
    let stored_key = hmac salted_password client_key |> sha256 in
    let auth_message =
      String.concat
        ~sep:","
        [ t.client_first_message_bare
        ; server_first_message
        ; client_final_message_without_proof
        ]
    in
    let client_signature = hmac stored_key auth_message in
    let client_proof =
      hmac salted_password client_key |> xor_strings_exn client_signature
    in
    match t.server_first_message with
    | Some server_first_message ->
      Or_error.error_s
        [%message
          "SCRAM first server message is already populated?! (while processing first \
           server message) "
            (server_first_message : string)]
    | None ->
      (match t.client_final_message_without_proof with
       | Some client_final_message_without_proof ->
         Or_error.error_s
           [%message
             "SCRAM client final message (without proof) was already recorded?! (while \
              trying to send client message)"
               (client_final_message_without_proof : string)]
       | None ->
         t.server_first_message <- Some server_first_message;
         t.client_final_message_without_proof <- Some client_final_message_without_proof;
         t.salted_password <- Some salted_password;
         Ok
           [%string
             "%{client_final_message_without_proof},p=%{Base64.encode_string \
              client_proof}"]))
;;

let verify_server_final t ~server_final_message =
  let%bind.Or_error attributes =
    parse_attributes server_final_message ~expected_attributes:[ 'e'; 'v' ]
  in
  match find_attribute attributes 'e' with
  | Ok server_error ->
    Or_error.error_s [%message "SCRAM server returned an error" (server_error : string)]
  | Error _ ->
    let%bind.Or_error encoded_server_signature = find_attribute attributes 'v' in
    let%bind.Or_error server_signature =
      Or_error.try_with (fun () -> Base64.decode_exn encoded_server_signature)
      |> Result.map_error ~f:(fun error ->
        Error.tag_arg
          error
          "malformed SCRAM server signature"
          encoded_server_signature
          [%sexp_of: string])
    in
    let%bind.Or_error server_first_message =
      Or_error.of_option_lazy_string
        t.server_first_message
        ~error:(lazy "SCRAM server-final before server-first")
    in
    let%bind.Or_error client_final_message_without_proof =
      Or_error.of_option_lazy_string
        t.client_final_message_without_proof
        ~error:(lazy "SCRAM server-final before client-final")
    in
    let%bind.Or_error salted_password =
      Or_error.of_option_lazy_string
        t.salted_password
        ~error:(lazy "SCRAM server-final before salted password")
    in
    let auth_message =
      String.concat
        ~sep:","
        [ t.client_first_message_bare
        ; server_first_message
        ; client_final_message_without_proof
        ]
    in
    let expected_server_signature = hmac (hmac salted_password server_key) auth_message in
    if String.equal server_signature expected_server_signature
    then Ok ()
    else Or_error.error_s [%message "SCRAM server signature did not match"]
;;
