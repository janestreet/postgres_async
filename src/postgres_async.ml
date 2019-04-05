open Core
open Async
open Int.Replace_polymorphic_compare

module Notification_channel = Types.Notification_channel

(* https://www.postgresql.org/docs/current/protocol.html *)

(* The ivars here are filled whenever the state changes. *)
type state =
  | Open    of unit Ivar.t
  | Closing of unit Ivar.t
  | Failed  of Error.t
  | Closed_gracefully
[@@deriving sexp_of]

(* There are a couple of invariants we need to be careful of. First, changes to
   [t.state] need simultaneous ivar filling and/or reader & writer closing.

   Secondly, we need to be careful to catch synchronous writer exceptions and
   write flush messages to postgres.

   This module forbids the rest of the code from doing those things directly
   (by making [t.state] private and the writer only available via a helper
   function), so that by reading the short [module T] we can be confident the
   whole module handles this correctly. *)
module T : sig
  type opaque

  type t = private
    { writer : opaque
    ; reader : Reader.t
    ; mutable state : state
    ; sequencer : unit Sequencer.t
    ; runtime_parameters : string String.Table.t
    ; notification_buses : (string -> unit, read_write) Bus.t Notification_channel.Table.t
    ; backend_key : Types.backend_key Set_once.t
    }
  [@@deriving sexp_of]

  val create_internal : Reader.t -> Writer.t -> t

  val failed : t -> Error.t -> unit

  val close_finished : t -> unit Or_error.t Deferred.t
  val close : t -> unit Or_error.t Deferred.t

  type to_flush_or_not =
    | Not_required
    | Write_afterwards

  (** [flush_message] is a bit of a weird argument for this function/weird feature
      to give this part of the code, but it reminds us to always consider the need
      to flush. *)
  val catch_write_errors
    :  t
    -> f:(Writer.t -> unit)
    -> flush_message:to_flush_or_not
    -> unit

  val bytes_pending_in_writer_buffer : t -> int
  val wait_for_writer_buffer_to_be_empty : t -> unit Deferred.t
end = struct
  type opaque = Writer.t

  type t =
    { writer : (Writer.t [@sexp.opaque])
    ; reader : (Reader.t [@sexp.opaque])
    ; mutable state : state
    ; sequencer : unit Sequencer.t
    ; runtime_parameters : string String.Table.t
    ; notification_buses : (string -> unit, read_write) Bus.t Notification_channel.Table.t
    ; backend_key : Types.backend_key Set_once.t
    }
  [@@deriving sexp_of]

  let cleanup_resources t =
    let%bind () = Writer.close t.writer ~force_close:Deferred.unit in
    let%bind () = Reader.close t.reader in
    Hashtbl.clear t.notification_buses;
    Hashtbl.clear t.runtime_parameters;
    return ()


  let failed t err =
    don't_wait_for (cleanup_resources t);
    match t.state with
    | Failed _ | Closed_gracefully -> ()
    | Open signal ->
      t.state <- Failed err;
      Ivar.fill signal ()
    | Closing signal ->
      t.state <- Failed err;
      Ivar.fill signal ()

  let create_internal reader writer =
    { reader
    ; writer
    ; state = Open (Ivar.create ())
    ; sequencer = Sequencer.create ~continue_on_error:true ()
    ; runtime_parameters = String.Table.create ()
    ; notification_buses = Notification_channel.Table.create ~size:1 ()
    ; backend_key = Set_once.create ()
    }

  let ensure_is_eof reader =
    (* If we try to read from a closed reader, we'll get an exn. This could
       happen if the user forces a stop while we're gracefully closing. *)
    match%bind
      Clock.with_timeout
        (sec 1.)
        (Monitor.try_with (fun () -> Reader.read_char reader))
    with
    | `Timeout -> return (Or_error.error_string "EOF expected, but instead stuck")
    | `Result (Error exn) -> return (Or_error.of_exn exn)
    | `Result (Ok (`Ok _)) -> return (Or_error.error_string "EOF expected, but there was still data")
    | `Result (Ok `Eof) -> return (Ok `Closed_gracefully)

  let start_close__gracefully_if_possible t =
    match t.state with
    | Failed _ | Closed_gracefully | Closing _ -> ()
    | Open left_open_state ->
      Ivar.fill left_open_state ();
      t.state <- Closing (Ivar.create ());
      match Protocol.Frontend.Writer.terminate t.writer with
      | exception exn -> failed t (Error.of_exn exn)
      | () ->
        don't_wait_for (
          match%bind ensure_is_eof t.reader with
          | Error err ->
            failed t err;
            Deferred.unit
          | Ok `Closed_gracefully ->
            let%bind () = cleanup_resources t in
            match t.state with
            | Open _ | Closed_gracefully -> assert false
            | Failed _ ->
              (* e.g., if the Writer had an asynchronous exn while the reader was being closed,
                 we might have called [failed t] and filled this ivar already. *)
              return ()
            | Closing signal ->
              Ivar.fill signal ();
              t.state <- Closed_gracefully;
              return ()
        )

  let rec close_finished t =
    match t.state with
    | Failed err -> return (Error err)
    | Closed_gracefully -> return (Ok ())
    | Closing changed | Open changed ->
      let%bind () = Ivar.read changed in
      close_finished t

  let close t =
    start_close__gracefully_if_possible t;
    close_finished t

  type to_flush_or_not =
    | Not_required
    | Write_afterwards

  let catch_write_errors t ~f ~flush_message : unit =
    match f t.writer with
    | exception exn -> failed t (Error.of_exn exn)
    | () ->
      match flush_message with
      | Not_required -> ()
      | Write_afterwards ->
        match Protocol.Frontend.Writer.flush t.writer with
        | exception exn -> failed t (Error.of_exn exn)
        | () -> ()

  let bytes_pending_in_writer_buffer t = Writer.bytes_to_write t.writer
  let wait_for_writer_buffer_to_be_empty t = Writer.flushed t.writer
end

include T

let notification_bus t channel =
  Hashtbl.find_or_add t.notification_buses channel
    ~default:(fun () ->
      Bus.create
        [%here]
        Arity1
        ~on_subscription_after_first_write:Allow
        ~on_callback_raise:Error.raise
    )

(* [Message_reading] hides the helper functions of [read_messages] from the below. *)
module Message_reading : sig
  type 'a handle_message_result =
    | Stop of 'a
    | Continue
    | Protocol_error of Error.t

  (** [read_messages] and will handle and dispatch the three asynchronous
      message types for you; you should never see them. *)
  type 'a handle_message
    =  Protocol.Backend.constructor
    -> (read, Iobuf.seek) Iobuf.t
    -> 'a handle_message_result

  type 'a read_messages_result =
    | Connection_closed of Error.t
    | Done of 'a

  val read_messages
    :  ?pushback:(unit -> unit Deferred.t)
    -> t
    -> handle_message:'a handle_message
    -> 'a read_messages_result Deferred.t
end = struct
  type 'a handle_message_result =
    | Stop of 'a
    | Continue
    | Protocol_error of Error.t

  let max_message_length = 1024 * 1024

  let handle_notice_response iobuf =
    match Protocol.Backend.NoticeResponse.consume iobuf with
    | Error err -> Protocol_error err
    | Ok info ->
      Log.Global.sexp ~level:`Info [%message "Postgres NoticeResponse" (info : Info.t)];
      Continue

  let handle_parameter_status t iobuf =
    match Protocol.Backend.ParameterStatus.consume iobuf with
    | Error err -> Protocol_error err
    | Ok { key; data } ->
      Hashtbl.set t.runtime_parameters ~key ~data;
      Continue

  let handle_notification_response t iobuf =
    match Protocol.Backend.NotificationResponse.consume iobuf with
    | Error err -> Protocol_error err
    | Ok { pid = _; channel; payload } ->
      let bus = notification_bus t channel in
      (match Bus.num_subscribers bus with
       | 0 ->
         Log.Global.sexp
           ~level:`Error
           [%message
             "Postgres NotificationResponse on channel that no callbacks \
              are listening to"
               (channel : Notification_channel.t)
           ]
       | _ ->
         Bus.write bus payload);
      Continue

  type 'a handle_message
    =  Protocol.Backend.constructor
    -> (read, Iobuf.seek) Iobuf.t
    -> 'a handle_message_result

  let handle_chunk t ~handle_message ~pushback =
    let stop_error sexp = return (`Stop (`Protocol_error (Error.create_s sexp))) in
    let rec loop iobuf =
      let hi_bound = Iobuf.Hi_bound.window iobuf in
      match Protocol.Backend.focus_on_message iobuf with
      | Error `Too_short ->
        if Iobuf.length iobuf > max_message_length
        then stop_error [%message "Message too long" ~iobuf_length:(Iobuf.length iobuf : int)]
        else (
          let%bind () = pushback () in
          return `Continue)
      | Error (`Unknown_message_type other) ->
        stop_error [%message "Unrecognised message type character" (other : char)]
      | Ok message_type ->
        let res =
          let iobuf = Iobuf.read_only iobuf in
          (* Notice Response and Parameter Status may happen at any time. *)
          match message_type with
          | NoticeResponse -> handle_notice_response iobuf
          | ParameterStatus -> handle_parameter_status t iobuf
          | NotificationResponse -> handle_notification_response t iobuf
          | _ -> handle_message message_type iobuf
        in
        let res =
          match res with
          | Protocol_error _ as res -> res
          | Stop _ | Continue as res ->
            match Iobuf.is_empty iobuf with
            | true -> res
            | false ->
              Protocol_error (Error.create_s (
                [%message
                  "handle_message did not consume entire iobuf"
                    (message_type : Protocol.Backend.constructor)
                    ~bytes_remaining:(Iobuf.length iobuf : int)
                ]
              ))
        in
        Iobuf.bounded_flip_hi iobuf hi_bound;
        match res with
        | Protocol_error err -> return (`Stop (`Protocol_error err))
        | Continue -> loop iobuf
        | Stop s -> return (`Stop (`Done s))
    in
    Staged.stage loop

  let no_pushback () = return ()

  type 'a read_messages_result =
    | Connection_closed of Error.t
    | Done of 'a

  let can't_read_because_closed =
    lazy (
      return (Connection_closed (Error.of_string "can't read: closed or closing")))

  let read_messages ?(pushback=no_pushback) t ~handle_message =
    let handle_chunk = Staged.unstage (handle_chunk t ~handle_message ~pushback) in
    (* We shouldn't ever get an exn here. In particular, closing the reader
       underneath us should simply produce `Eof. *)
    match t.state with
    | Failed err ->
      return (Connection_closed err)
    | Closed_gracefully | Closing _ ->
      force can't_read_because_closed
    | Open _ ->
      (* In case of some failure (writer failure, including asynchronous) the
         reader will be closed and reads will return Eof. So, after the read
         result is determined, we check [t.state], so that we can give a better
         error message than "unexpected EOF". *)
      let%bind res = Reader.read_one_iobuf_at_a_time t.reader ~handle_chunk in
      match t.state with
      | Failed err ->
        return (Connection_closed err)
      | Closing _ | Closed_gracefully ->
        force can't_read_because_closed
      | Open _ ->
        let res =
          match res with
          | `Stopped s -> s
          | `Eof_with_unconsumed_data data ->
            `Protocol_error (Error.create_s (
              [%message
                "Unexpected EOF"
                  ~unconsumed_bytes:(String.length data : int)
              ]
            ))
          | `Eof ->
            `Protocol_error (
              Error.create_s [%message "Unexpected EOF (no unconsumed messages)"]
            )
        in
        match res with
        | `Protocol_error err ->
          failed t err;
          return (Connection_closed err)
        | `Done res ->
          return (Done res)
end

include Message_reading

let protocol_error_s sexp = Protocol_error (Error.create_s sexp)

let unexpected_msg_type msg_type state =
  protocol_error_s
    [%message
      "Unexpected message type"
        (msg_type : Protocol.Backend.constructor)
        (state : Sexp.t)
    ]

let login t ~user ~password ~gss_krb_token ~database =
  catch_write_errors t ~flush_message:Not_required ~f:(fun writer ->
    Protocol.Frontend.Writer.startup_message writer { user; database }
  );
  read_messages t ~handle_message:(fun msg_type iobuf ->
    match msg_type with
    | AuthenticationRequest ->
      let module Q = Protocol.Backend.AuthenticationRequest in
      (match Q.consume iobuf with
       | Error err -> Protocol_error err
       | Ok Ok -> Continue
       | Ok (MD5Password { salt }) ->
         (match password with
          | Some password ->
            let d s = Md5.to_hex (Md5.digest_string s) in
            let md5_hex = "md5" ^ d (d (password ^ user) ^ salt) in
            catch_write_errors t ~flush_message:Not_required ~f:(fun writer ->
              Protocol.Frontend.Writer.password_message
                writer
                (Cleartext_or_md5_hex md5_hex)
            );
            Continue
          | None ->
            let s = "Server requested (md5) password, but no password was provided" in
            Stop (Or_error.error_string s))
       | Ok GSS ->
         (match gss_krb_token with
          | Some token ->
            catch_write_errors t ~flush_message:Not_required ~f:(fun writer ->
              Protocol.Frontend.Writer.password_message writer (Gss_binary_blob token)
            );
            Continue
          | None ->
            let s = "Server requested GSS auth, but no gss_krb_token was provided" in
            Stop (Or_error.error_string s))
       | Ok other ->
         let s = sprintf !"Server wants unimplemented auth subtype: %{sexp:Q.t}" other in
         Stop (Or_error.error_string s))
    | BackendKeyData ->
      (match Protocol.Backend.BackendKeyData.consume iobuf with
       | Error err -> Protocol_error err
       | Ok data ->
         match Set_once.set t.backend_key [%here] data with
         | Ok () -> Continue
         | Error _ -> protocol_error_s [%sexp "duplicate BackendKeyData messages"])
    | ReadyForQuery ->
      let module Q = Protocol.Backend.ReadyForQuery in
      (match Q.consume iobuf with
       | Error err -> Protocol_error err
       | Ok Idle -> Stop (Ok ())
       | Ok st ->
         protocol_error_s [%message "Unexpected initial transaction status" (st : Q.t)])
    | ErrorResponse ->
      (match Protocol.Backend.ErrorResponse.consume iobuf with
       | Error err -> Protocol_error err
       | Ok err -> Stop (Error err))
    | msg_type ->
      unexpected_msg_type msg_type [%sexp "logging in"]
  )

let default_user = Memo.unit (fun () -> Monitor.try_with_or_error Unix.getlogin)

type packed_where_to_connect =
  | Where_to_connect : 'a Tcp.Where_to_connect.t -> packed_where_to_connect

let default_where_to_connect =
  lazy (
    Where_to_connect (Tcp.Where_to_connect.of_file "/run/postgresql/.s.PGSQL.5432"))

let connect
      ?(interrupt=Deferred.never ())
      ?server
      ?user
      ?password
      ?gss_krb_token
      ~database
      ()
  =
  let Where_to_connect server =
    match server with
    | Some x -> Where_to_connect x
    | None -> force default_where_to_connect
  in
  let%bind (user : string Or_error.t) =
    match user with
    | Some u -> return (Ok u)
    | None -> default_user ()
  in
  match user with
  | Error _ as err -> return err
  | Ok user ->
    match%bind Monitor.try_with_or_error (fun () -> Tcp.connect ~interrupt server) with
    | Error _ as err -> return err
    | Ok (_sock, reader, writer) ->
      let t = create_internal reader writer in
      let writer_failed = Monitor.detach_and_get_next_error (Writer.monitor writer) in
      upon writer_failed (fun exn ->
        failed t (Error.create_s [%message "Writer failed asynchronously" (exn : Exn.t)])
      );
      let login_failed err =
        failed t err;
        return (Error err)
      in
      match%bind
        choose
          [ choice (Clock.after (sec 10.))                        (fun () -> `Timeout)
          ; choice interrupt                                      (fun () -> `Interrupt)
          ; choice (login t ~user ~password ~gss_krb_token ~database) (fun l -> `Result l)
          ]
      with
      | `Timeout   -> login_failed (Error.of_string "timeout while logging in")
      | `Interrupt -> login_failed (Error.of_string "login interrupted")
      | `Result (Connection_closed err) -> return (Error err)
      | `Result (Done (Error err))      -> login_failed err
      | `Result (Done (Ok ()))          -> return (Ok t)

let with_connection ?interrupt ?server ?user ?password ?gss_krb_token ~database f =
  match%bind connect ?interrupt ?server ?user ?password ?gss_krb_token ~database () with
  | Error _ as e -> return e
  | Ok t ->
    match%bind Monitor.try_with (fun () -> f t) with
    | Ok _ as ok_res ->
      let%bind (_ : unit Or_error.t) = close t in
      return ok_res
    | Error exn ->
      don't_wait_for (
        let%bind (_ : unit Or_error.t) = close t in
        return ()
      );
      raise exn

(* We use the extended query protocol rather than the simple query protocol
   because it provides support for [parameters] (via the 'bind' message), and
   because it guarantees that only a SQL statement is executed per query, which
   simplifies the state machine we need to use to handle the responses
   significantly.

   We don't currently take advantage of named statements or portals, we just
   use the 'unnamed' ones. We don't take advantage of the ability to bind a
   statement twice or partially execute a portal; we just send the full
   parse-bind-describe-execute sequence unconditionally in one go.

   Ultimately, one needs to read the "message flow/extended query" section of
   the documentation:

   https://www.postgresql.org/docs/10/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY

   What follows is a brief summary, and then a description of how the different
   functions fit in.

   When any error occurs, the server will report the error and tehen discard
   messages until a 'sync' message. This means that we can safely send
   parse-bind-describe-execute in one go and then read all the responses,
   because if one step fails the remainder will be ignored. So, we do this
   because it improves latency.

   [parse_and_start_executing_query] instructs the server to parse the query,
   bind the parameters, describe the type of the rows that will be produced.
   It then reads response messages, walking through a state machine. The first
   three states are easy, because we expect either the relevant response
   messages (ParseComplete, BindComplete, RowDescription|NoData), or an
   ErrorResponse.

   After that, things are trickier. We don't know what string was in the query
   and whether or not the query emits no data (e.g., a DELETE), or if server is
   going to send us rows, or if it's going to try and COPY IN/OUT, etc.

   [parse_and_start_executing_query] detects which case we're in and returns a
   value of type [setup_loop_result] indicating this. It's a little weird
   because we don't want to consume any of the actual data (if applicable) in
   that funciton, just detect the mode.

   If the response to the [Describe] message was a [RowDescription], we know
   that the response to the [Execute] will be some [DataRow]s, so we don't look
   at any messages after that (i.e., in that case we don't look at the response
   to [Execute] in this function).

   However if the response to [Describe] was [NoData], then we _have_ to look at
   the first message in response to the [Execute] message to know which case
   we're in. _Fortunately_, the first message in all the other cases does not
   actually contain any data, so we can do what we want (detect mode and return).

   Here they are:

   - [About_to_deliver_rows] e.g., a SELECT. We know we're in this case iff the
     response to our [Describe] message was a [RowDescription], and we don't look
     at the response to the [Execute] message; the caller is responsible for
     consuming the [DataRow]s.
   - [About_to_copy_out] i.e., COPY ... TO STDOUT. We're here iff we get a
     [CopyOutResponse] in response to our [Execute].
   - [Ready_to_copy_in] i.e., COPY ... FROM STDIN. We're here iff we get a
     [CopyInResponse]. We return, and the caller is responsible for sending
     a load of [CopyData] messages etc.
   - [EmptyQuery] i.e., the query was the empty string. We're here if we get
     a [EmptyQueryResponse], at which point we're done (and can sync); we do not
     expect a [CommandComplete].
   - [Command_complete_without_output], e.g. a DELETE. We're here if we just get
     [CommandComplete] straight away.

   Note that it's also possible for the [About_to_deliver_rows] case to complete
   without output. The difference is that one corresponds to receiving
   [RowDescription], [CommandComplete] and the other [NoData], [CommandComplete]. *)
type setup_loop_state =
  | Parsing
  | Binding
  | Describing
  | Executing
[@@deriving sexp_of]

type setup_loop_result =
  | About_to_deliver_rows of { column_names : string array }
  | About_to_copy_out
  | Ready_to_copy_in of Protocol.Backend.CopyInResponse.t
  | Empty_query
  | Command_complete_without_output
  | Remote_reported_error of Error.t

let parse_and_start_executing_query t query_string ~parameters =
  match t.state with
  | Failed err ->
    return (Connection_closed (
      Error.create_s
        [%message
          "query issued against previously-failed connection"
            ~original_error:(err : Error.t)]))
  | Closing _ | Closed_gracefully ->
    return (Connection_closed (
      Error.of_string "query issued against connection closed by user"))
  | Open _ ->
    catch_write_errors t ~flush_message:Write_afterwards ~f:(fun writer ->
      Protocol.Frontend.Writer.parse
        writer
        { destination = Types.Statement_name.unnamed
        ; query = query_string
        };
      Protocol.Frontend.Writer.bind
        writer
        { destination = Types.Portal_name.unnamed
        ; statement = Types.Statement_name.unnamed
        ; parameters
        };
      Protocol.Frontend.Writer.describe
        writer
        (Portal Types.Portal_name.unnamed);
      Protocol.Frontend.Writer.execute
        writer
        { portal = Types.Portal_name.unnamed
        ; limit = Unlimited
        };
    );
    let state = ref Parsing in
    let unexpected_msg_type msg_type =
      unexpected_msg_type msg_type [%sexp (state : setup_loop_state ref)]
    in
    read_messages t ~handle_message:(fun msg_type iobuf ->
      match (!state, msg_type) with
      | (state, ErrorResponse) ->
        (match Protocol.Backend.ErrorResponse.consume iobuf with
         | Error err -> Protocol_error err
         | Ok err ->
           let s = [%message "Postgres Server Error" (state : setup_loop_state) ~_:(err : Error.t)] in
           Stop (Remote_reported_error (Error.create_s s)))
      | (Parsing, ParseComplete) ->
        let () = Protocol.Backend.ParseComplete.consume iobuf in
        state := Binding;
        Continue
      | (Parsing, msg_type) ->
        unexpected_msg_type msg_type
      | (Binding, BindComplete) ->
        let () = Protocol.Backend.BindComplete.consume iobuf in
        state := Describing;
        Continue
      | (Binding, msg_type) ->
        unexpected_msg_type msg_type
      | (Describing, RowDescription) ->
        (match Protocol.Backend.RowDescription.consume iobuf with
         | Error err -> Protocol_error err
         | Ok description ->
           let column_names =
             Array.map description ~f:(fun { name; format = `Text } -> name)
           in
           Stop (About_to_deliver_rows { column_names }))
      | (Describing, NoData) ->
        let () = Protocol.Backend.NoData.consume iobuf in
        state := Executing;
        Continue
      | (Describing, msg_type) ->
        unexpected_msg_type msg_type
      | (Executing, EmptyQueryResponse) ->
        let () = Protocol.Backend.EmptyQueryResponse.consume iobuf in
        Stop Empty_query
      | (Executing, CommandComplete) ->
        (match Protocol.Backend.CommandComplete.consume iobuf with
         | Error err -> Protocol_error err
         | Ok (_ : string) -> Stop Command_complete_without_output)
      | (Executing, CopyInResponse) ->
        (match Protocol.Backend.CopyInResponse.consume iobuf with
         | Error err -> Protocol_error err
         | Ok details -> Stop (Ready_to_copy_in details))
      | (Executing, CopyOutResponse) ->
        (match Protocol.Backend.CopyOutResponse.consume iobuf with
         | Error err -> Protocol_error err
         | Ok _ -> Stop About_to_copy_out)
      | (Executing, CopyBothResponse) ->
        (* CopyBothResponse is only used for streaming replication, which we do not initiate. *)
        unexpected_msg_type msg_type
      | (Executing, msg_type) ->
        unexpected_msg_type msg_type
    )

(* really the return type of [f] should be [Protocol_error _ | Continue], but it's
   convenient to re-use [handle_message_result] and use [Nothing.t] to 'delete' the
   third constructor... *)
let read_datarows t ~pushback ~f =
  read_messages t ?pushback ~handle_message:(fun msg_type iobuf ->
    match msg_type with
    | DataRow ->
      (match f ~datarow_iobuf:iobuf with
       | Protocol_error _ | Continue as x -> x
       | Stop (_ : Nothing.t) -> .)
    | ErrorResponse ->
      (match Protocol.Backend.ErrorResponse.consume iobuf with
       | Error err -> Protocol_error err
       | Ok err ->
         let err =
           Error.create_s
             [%message "Error during query execution (despite parsing ok)" ~_:(err : Error.t)]
         in
         Stop (Error err))
    | CommandComplete ->
      (match Protocol.Backend.CommandComplete.consume iobuf with
       | Error err -> Protocol_error err
       | Ok (_ : string) -> Stop (Ok ()))
    | msg_type ->
      unexpected_msg_type msg_type [%sexp "reading DataRows"]
  )

let drain_datarows t =
  let count = ref 0 in
  let f ~datarow_iobuf =
    incr count;
    Protocol.Backend.DataRow.skip datarow_iobuf;
    Continue
  in
  match%bind read_datarows t ~pushback:None ~f with
  | Connection_closed _ as e -> return e
  | Done (Ok () | Error _) -> return (Done !count)

let drain_copy_out t =
  let seen_copy_done = ref false in
  read_messages t ~handle_message:(fun msg_type iobuf ->
    match msg_type with
    | ErrorResponse ->
      (* [ErrorResponse] terminates copy-out mode; no separate [CopyDone] is required. *)
      (match Protocol.Backend.ErrorResponse.consume iobuf with
       | Error err -> Protocol_error err
       | Ok _ -> Stop ())
    | CopyData ->
      (match !seen_copy_done with
       | true -> protocol_error_s [%sexp "CopyData message after CopyDone?"]
       | false ->
         Protocol.Backend.CopyData.skip iobuf;
         Continue)
    | CopyDone ->
      Protocol.Backend.CopyDone.consume iobuf;
      seen_copy_done := true;
      Continue
    | CommandComplete ->
      (match Protocol.Backend.CommandComplete.consume iobuf with
       | Error err -> Protocol_error err
       | Ok (_ : string) ->
         match !seen_copy_done with
         | false -> protocol_error_s [%sexp "CommandComplete before CopyDone?"]
         | true -> Stop ())
    | msg_type ->
      unexpected_msg_type msg_type [%sexp "draining copy-out mode"]
  )

let abort_copy_in t ~reason =
  catch_write_errors t ~flush_message:Write_afterwards ~f:(fun writer ->
    Protocol.Frontend.Writer.copy_fail writer { reason };
  );
  read_messages t ~handle_message:(fun msg_type iobuf ->
    match msg_type with
    | ErrorResponse ->
      (match Protocol.Backend.ErrorResponse.consume iobuf with
       | Error err -> Protocol_error err
       | Ok _ -> Stop ())
    | msg_type ->
      unexpected_msg_type msg_type [%sexp "aborting copy-in mode"]
  )

let sync_after_query t =
  catch_write_errors t ~flush_message:Not_required ~f:(fun writer ->
    Protocol.Frontend.Writer.sync writer
  );
  read_messages t ~handle_message:(fun msg_type iobuf ->
    match msg_type with
    | ReadyForQuery ->
      (match Protocol.Backend.ReadyForQuery.consume iobuf with
       | Error err -> Protocol_error err
       | Ok (Idle | In_transaction | In_failed_transaction) -> Stop ())
    | ErrorResponse ->
      (match Protocol.Backend.ErrorResponse.consume iobuf with
       | Error err -> Protocol_error err
       | Ok e -> protocol_error_s [%message "response to Sync was an error" ~_:(e : Error.t)])
    | msg_type ->
      unexpected_msg_type msg_type [%sexp "synchronising after query"]
  )

let internal_query t ?(parameters=[||]) ?pushback query_string ~handle_row =
  let%bind result =
    match%bind parse_and_start_executing_query t query_string ~parameters with
    | Connection_closed _ as err -> return err
    | Done About_to_copy_out ->
      let%bind (Connection_closed _ | Done ()) = drain_copy_out t in
      return (Done (error_s [%message "COPY TO STDOUT is not appropriate for [Postgres_async.query]"]))
    | Done (Ready_to_copy_in _) ->
      let reason = "COPY FROM STDIN is not appropriate for [Postgres_async.query]" in
      let%bind (Connection_closed _ | Done ()) = abort_copy_in t ~reason in
      return (Done (Or_error.error_string reason))
    | Done (Remote_reported_error error) ->
      return (Done (Error error))
    | Done (Empty_query | Command_complete_without_output) ->
      return (Done (Ok ()))
    | Done (About_to_deliver_rows { column_names }) ->
      read_datarows t ~pushback ~f:(fun ~datarow_iobuf:iobuf ->
        match Protocol.Backend.DataRow.consume iobuf with
        | Error err -> Protocol_error err
        | Ok values ->
          handle_row ~column_names ~values;
          Continue
      )
  in
  (* Note that if we're not within a BEGIN/END block, [sync_after_query] commits the
     transaction. *)
  let%bind sync_result = sync_after_query t in
  match (result, sync_result) with
  | (Connection_closed err, _)
  | (Done (Error err), _)
  | (_, Connection_closed err) -> return (Error err)
  | (Done (Ok ()), Done ()) -> return (Ok ())

(* [query] wraps [internal_query], acquiring the sequencer lock and keeping
   the user's exceptions away from trashing our state. *)
let query t ?parameters ?pushback query_string ~handle_row =
  let callback_raised = ref false in
  let handle_row ~column_names ~values =
    match !callback_raised with
    | true -> ()
    | false ->
      match handle_row ~column_names ~values with
      | () -> ()
      | exception exn ->
        (* it's important that we drain (and discard) the remaining rows. *)
        Monitor.send_exn (Monitor.current ()) exn;
        callback_raised := true
  in
  let%bind result =
    Throttle.enqueue t.sequencer (fun () ->
      internal_query t ?parameters ?pushback query_string ~handle_row
    )
  in
  match !callback_raised with
  | true -> Deferred.never ()
  | false -> return result

(* [query_expect_no_data] doesn't need the separation that [internal_query]
   and [query] have because there's no callback to wrap and it's easy enough to
   just throw the sequencer around the whole function. *)
let query_expect_no_data t ?(parameters=[||]) query_string =
  Throttle.enqueue t.sequencer (fun () ->
    let%bind result =
      match%bind parse_and_start_executing_query t query_string ~parameters with
      | Connection_closed _ as err -> return err
      | Done About_to_copy_out ->
        let%bind (Connection_closed _ | Done ()) = drain_copy_out t in
        return (Done (error_s [%message "[Postgres_async.query_expect_no_data]: query attempted COPY OUT"]))
      | Done (Ready_to_copy_in _) ->
        let reason = "[Postgres_async.query_expect_no_data]: query attempted COPY IN" in
        let%bind (Connection_closed _ | Done ()) = abort_copy_in t ~reason in
        return (Done (Or_error.error_string reason))
      | Done (About_to_deliver_rows _ ) ->
        (match%bind drain_datarows t with
         | Connection_closed _ as err -> return err
         | Done 0 -> return (Done (Ok ()))
         | Done _ -> return (Done (error_s [%message "query unexpectedly produced rows"])))
      | Done (Remote_reported_error error) ->
        return (Done (Error error))
      | Done (Empty_query | Command_complete_without_output) ->
        return (Done (Ok ()))
    in
    let%bind sync_result = sync_after_query t in
    match (result, sync_result) with
    | (Connection_closed err, _)
    | (Done (Error err), _)
    | (_, Connection_closed err) -> return (Error err)
    | (Done (Ok ()), Done ()) -> return (Ok ())
  )

type 'a feed_data_result =
  | Abort of { reason : string }
  | Wait of unit Deferred.t
  | Data of 'a
  | Finished

let query_did_not_initiate_copy_in =
  lazy (
    return (Done (error_s [%message "Query did not initiate copy-in mode"])))

(* [internal_copy_in_raw] is to [copy_in_raw] as [internal_query] is to [query].
   Sequencer and exception handling. *)
let internal_copy_in_raw t ?(parameters=[||]) query_string ~feed_data =
  let%bind result =
    match%bind parse_and_start_executing_query t query_string ~parameters with
    | Connection_closed _ as err -> return err
    | Done About_to_copy_out ->
      let%bind (Connection_closed _ | Done ()) = drain_copy_out t in
      return (Done (error_s [%message "COPY TO STDOUT is not appropriate for [Postgres_async.query]"]))
    | Done (Empty_query | Command_complete_without_output) ->
      force query_did_not_initiate_copy_in
    | Done (About_to_deliver_rows _) ->
      let%bind (Connection_closed _ | Done (_ : int)) = drain_datarows t in
      force query_did_not_initiate_copy_in
    | Done (Remote_reported_error error) ->
      return (Done (Error error))
    | Done (Ready_to_copy_in _) ->
      let sent_copy_done = ref false in
      let (response_deferred : unit Or_error.t read_messages_result Deferred.t) =
        read_messages t ~handle_message:(fun msg_type iobuf ->
          match msg_type with
          | ErrorResponse ->
            (match Protocol.Backend.ErrorResponse.consume iobuf with
             | Error err -> Protocol_error err
             | Ok err -> Stop (Error err))
          | CommandComplete ->
            (match Protocol.Backend.CommandComplete.consume iobuf with
             | Error err -> Protocol_error err
             | Ok (_res : string) ->
               match !sent_copy_done with
               | false -> protocol_error_s [%sexp "CommandComplete response before we sent CopyDone?"]
               | true -> Stop (Ok ()))
          | msg_type ->
            unexpected_msg_type msg_type [%sexp "copying in"]
        )
      in
      let%bind () =
        let rec loop () =
          match feed_data () with
          | Abort { reason } ->
            catch_write_errors t ~flush_message:Write_afterwards ~f:(fun writer ->
              Protocol.Frontend.Writer.copy_fail writer { reason };
            );
            return ()
          | Wait deferred ->
            continue ~after:deferred
          | Data payload ->
            catch_write_errors t ~flush_message:Not_required ~f:(fun writer ->
              Protocol.Frontend.Writer.copy_data writer payload
            );
            (match bytes_pending_in_writer_buffer t > 128 * 1024 * 1024 with
             | false -> loop ()
             | true -> continue ~after:(wait_for_writer_buffer_to_be_empty t))
          | Finished ->
            sent_copy_done := true;
            catch_write_errors t ~flush_message:Write_afterwards ~f:(fun writer ->
              Protocol.Frontend.Writer.copy_done writer;
            );
            return ()
        and continue ~after =
          let%bind () = after in
          (* check for early termination; not necessary for correctness, just nice. *)
          match Deferred.peek response_deferred with
          | None -> loop ()
          | Some (Connection_closed _ | Done (Error _)) -> return ()
          | Some (Done (Ok ())) ->
            failwith "BUG: response_deferred is (Done (Ok ())), but we never set !sent_copy_done?"
        in
        loop ()
      in
      response_deferred
  in
  let%bind sync_result = sync_after_query t in
  match (result, sync_result) with
  | (Connection_closed err, _)
  | (Done (Error err), _)
  | (_, Connection_closed err) -> return (Error err)
  | (Done (Ok ()), Done ()) -> return (Ok ())

let copy_in_raw t ?parameters query_string ~feed_data =
  let callback_raised = ref false in
  let feed_data () =
    (* We shouldn't be called again after [Abort] *)
    assert (not !callback_raised);
    match feed_data () with
    | Abort _ | Wait _ | Data _ | Finished as x -> x
    | exception exn ->
      Monitor.send_exn (Monitor.current ()) exn;
      callback_raised := true;
      Abort { reason = "feed_data callback raised" }
  in
  let%bind result =
    Throttle.enqueue t.sequencer (fun () ->
      internal_copy_in_raw t ?parameters query_string ~feed_data
    )
  in
  match !callback_raised with
  | true -> Deferred.never ()
  | false -> return result

(* [copy_in_rows] builds on [copy_in_raw] and clearly doesn't raise exceptions
   in the callback wrapper, so we can just rely on [copy_in_raw] to handle
   sequencing and exceptions here. *)
let copy_in_rows t ~table_name ~column_names ~feed_data =
  let query_string = String_escaping.Copy_in.query ~table_name ~column_names in
  copy_in_raw t query_string ~feed_data:(fun () ->
    match feed_data () with
    | Abort _ | Wait _ | Finished as x -> x
    | Data row -> Data (String_escaping.Copy_in.row_to_string row)
  )
