open Core
open Async
open Int.Replace_polymorphic_compare

module Notification_channel = Types.Notification_channel

(* https://www.postgresql.org/docs/current/protocol.html *)

type state =
  | Open
  | Closing
  | Failed  of Error.t
  | Closed_gracefully
[@@deriving sexp_of]

(* There are a couple of invariants we need to be careful of:

   - changes to [t.state] need simultaneous ivar filling and/or reader & writer closing.
   - [t.state = Open _] must imply [t.reader] is not closed

   Further, we need to be careful to catch synchronous writer exceptions and write flush
   messages to postgres.

   We try and make sure these invariants hold by making some of the fields in [t] opaque
   to stuff outside of this small module, so that by reading the short [module T] we can
   be confident the whole module handles this correctly.

   (It's not perfect, e.g. [t.reader] is exposed, but this isn't awful.) *)
module T : sig
  (* [opaque] makes [writer] and [state_changed] inaccessible outside of [T]. *)
  type 'a opaque

  type t = private
    { writer : Writer.t opaque
    ; reader : Reader.t
    ; mutable state : state
    ; state_changed : (unit, read_write) Bvar.t opaque
    ; sequencer : Query_sequencer.t
    ; runtime_parameters : string String.Table.t
    ; notification_buses : (string -> unit, read_write) Bus.t Notification_channel.Table.t
    ; backend_key : Types.backend_key Set_once.t
    }
  [@@deriving sexp_of]

  val create_internal : Reader.t -> Writer.t -> t

  val failed : t -> Error.t -> unit

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

  val close_finished : t -> unit Or_error.t Deferred.t
  val close : t -> unit Or_error.t Deferred.t
end = struct
  type 'a opaque = 'a

  type t =
    { writer : (Writer.t [@sexp.opaque])
    ; reader : (Reader.t [@sexp.opaque])
    ; mutable state : state
    ; state_changed : (unit, read_write) Bvar.t
    ; sequencer : Query_sequencer.t
    ; runtime_parameters : string String.Table.t
    ; notification_buses : (string -> unit, read_write) Bus.t Notification_channel.Table.t
    ; backend_key : Types.backend_key Set_once.t
    }
  [@@deriving sexp_of]

  let set_state t state =
    t.state <- state;
    Bvar.broadcast t.state_changed ()

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
    | Open | Closing -> set_state t (Failed err)

  let create_internal reader writer =
    { reader
    ; writer
    ; state = Open
    ; state_changed = Bvar.create ()
    ; sequencer = Query_sequencer.create ()
    ; runtime_parameters = String.Table.create ()
    ; notification_buses = Notification_channel.Table.create ~size:1 ()
    ; backend_key = Set_once.create ()
    }

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

  let do_close_gracefully_if_possible t =
    match t.state with
    | Failed _ | Closed_gracefully | Closing -> return ()
    | Open ->
      set_state t Closing;
      let closed_gracefully_deferred =
        Query_sequencer.enqueue t.sequencer (fun () ->
          catch_write_errors t ~flush_message:Not_required ~f:(fun writer ->
            Protocol.Frontend.Writer.terminate writer
          );
          (* we'll get an exception if the reader is already closed. *)
          match%bind Monitor.try_with (fun () -> Reader.read_char t.reader) with
          | Ok `Eof -> return (Ok ())
          | Ok (`Ok c) -> return (Or_error.errorf "EOF expected, but got '%c'" c)
          | Error exn -> return (Or_error.of_exn exn)
        )
      in
      match%bind Clock.with_timeout (sec 1.) closed_gracefully_deferred with
      | `Timeout ->
        failed t (Error.of_string "EOF expected, but instead stuck");
        return ()
      | `Result (Error err) ->
        failed t err;
        return ()
      | `Result (Ok ()) ->
        let%bind () = cleanup_resources t in
        match t.state with
        | Open | Closed_gracefully -> assert false
        | Failed _ ->
          (* e.g., if the Writer had an asynchronous exn while the reader was being
             closed, we might have called [failed t] and filled this ivar already. *)
          return ()
        | Closing ->
          set_state t Closed_gracefully;
          return ()

  let rec close_finished t =
    match t.state with
    | Failed err -> return (Error err)
    | Closed_gracefully -> return (Ok ())
    | Closing | Open ->
      let%bind () = Bvar.wait t.state_changed in
      close_finished t

  let close t =
    don't_wait_for (do_close_gracefully_if_possible t);
    close_finished t
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
      message types for you; you should never see them.

      [handle_message] is given a message type constructor, and an iobuf windowed on the
      payload of the message (that is, the message-type-specific bytes; the window does
      not include the type or message length header, they have already been consumed).

      [handle_message] must consume (as in [Iobuf.Consume]) all of the bytes of the
      message. *)
  type 'a handle_message
    =  Protocol.Backend.constructor
    -> (read, Iobuf.seek) Iobuf.t
    -> 'a handle_message_result

  type 'a read_messages_result =
    | Connection_closed of Error.t
    | Done of 'a

  (** NoticeResponse, and ParameterStatus and NotificationResponse are 'asynchronous
      messages' and are not associated with a specific request-response conversation.
      They can happen at any time.

      [read_messages] handles these for you, and does not show them to your
      [handle_message] callback.  *)
  val read_messages
    :  ?pushback:(unit -> unit Deferred.t)
    -> t
    -> handle_message:'a handle_message
    -> 'a read_messages_result Deferred.t

  (** If a message arrives while no request-response conversation (query or otherwise) is
      going on, use [consume_one_asynchronous_message] to eat it.

      If the message is one of those asynchronous messages, it will be handled. If it is
      some other message type, that is a protocol error and the connection will be closed.
      If the reader is actually at EOF, the connection will be closed with an error. *)
  val consume_one_asynchronous_message : t -> unit read_messages_result Deferred.t
end = struct
  type 'a handle_message_result =
    | Stop of 'a
    | Continue
    | Protocol_error of Error.t

  let max_message_length =
    let default = 1024 * 1024 in
    lazy (
      let override =
        (* for emergencies, in case 10M is a terrible default (seems very unlikely) *)
        let open Option.Let_syntax in
        let%bind v = Unix.getenv "POSTGRES_ASYNC_OVERRIDE_MAX_MESSAGE_LENGTH" in
        let%bind v = Option.try_with (fun () -> Int.of_string v) in
        let%bind v = Option.some_if (v > 0) v in
        Some v
      in
      Option.value override ~default)

  type 'a handle_message
    =  Protocol.Backend.constructor
    -> (read, Iobuf.seek) Iobuf.t
    -> 'a handle_message_result

  (* [Reader.read_one_iobuf_at_a_time] hands us 'chunks' to [handle_chunk]. A chunk is
     an iobuf, where the window of the iobuf is set to the data that has been pulled from
     the OS into the [Reader.t] but not yet consumed by the application. Said window
     'chunk' contains messages, potentially a partial message at the end in case e.g.
     the bytes of a message is split across two calls to the [read] syscall.

     Suppose the server has send us three 5-byte messages, "AAAAA", "BBBBB" and "CCCCC".
     Each message has a 5-byte header (containing its type and length), represented by
     'h'. Suppose further that the OS handed us 23 bytes when [Reader.t] called [read].

     {v
        0 bytes   10 bytes  20 bytes
        |         |         |
        hhhhhAAAAAhhhhhBBBBBhhh
       /--------window---------\
     v}

     We want to give each message to [handle_message], one at a time. So, first we must
     move the window on the iobuf so that it contains precisely the first message's
     payload, i.e., the bytes 'AAAAA', i.e., this iobuf but with the window set to [5,10).
     [Protocol.Backend.focus_on_message] does this by consuming the header 'hhhhh' to
     learn the type and length (thereby moving the window lo-bound up) and then resizing
     it (moving the hi-bound down) like so:

     {v
        hhhhhAAAAAhhhhhBBBBBhhh
            /-wdw-\
     v}

     Now we can call [handle_message] with this iobuf.

     The contract we have with [handle_message] is that it will use calls to functions in
     [Iobuf.Consume] to consume the bytes inside the window we set, that is, consume the
     entire message. As it consumes bytes of the message, the window lo-bound is moved up,
     so when it's done consuming the window is [10,10)

     {v
        hhhhhAAAAAhhhhhBBBBBhhh
                 /\
     v}

     and then [bounded_flip_hi] sets the window to [10,23) (since we remembered '23' in
     [chunk_hi_bound]).

     {v
        hhhhhAAAAAhhhhhBBBBBhhh
                 /-----window--\
     v}

     then, we recurse (focus on [15,20), call [handle_message], etc.), and recurse again.

     At this point, reading the 'message length' field tells us that we only have part of
     'C' in our buffer (three out of 5+5 bytes). So, we return from [handle_chunk]. The
     contract we have with [Reader.t] is that we will leave in the iobuf the data that we
     did not consume. It looks at the iobuf window to understand what's left, keeps only
     those three bytes of 'C' in its internal buffer, and uses [read] to refill the buffer
     before calling us again; hopefully at that point we'll have the whole of message 'C'.

     For this to work it is important that [handle_message] actually consumed the whole
     message, and did not move the window hi-bound. We _could_ save those bounds and
     forcibly restore them before doing the flip, however we prefer to instead just check
     that [handle_message] did this because the functions that parse messages are written
     to 'consume' them anyway, and checking that they actually did so is a good validation
     that we correctly understand and are thinking about all fields in the postgres
     protocol; not consuming all the bytes of the message indicates a potential parsing
     bug. That's what [check_window_after_handle_message] does.

     Note the check against [max_message_length]. Without it, if postgres told us that it
     was sending us an extremely long message, we'd return from [handle_chunk] constantly
     asking for refills, always thinking we needed more data and consuming all the RAM.
     Instead, we will just bail out rather than asking for refills if a message gets too
     long. The limit is arbitrary, but ought to be enough for anybody (with an environment
     variable escape hatch if we turn out to be wrong). *)

  let check_window_after_handle_message message_type iobuf ~message_hi_bound =
    match
      [%compare.equal: Iobuf.Hi_bound.t]
        (Iobuf.Hi_bound.window iobuf)
        message_hi_bound
    with
    | false ->
      error_s
        [%message
          "handle_message moved the hi-bound!"
            (message_type : Protocol.Backend.constructor)
        ]
    | true ->
      match Iobuf.is_empty iobuf with
      | false ->
        error_s
          [%message
            "handle_message did not consume entire iobuf"
              (message_type : Protocol.Backend.constructor)
              ~bytes_remaining:(Iobuf.length iobuf : int)
          ]
      | true ->
        Ok ()

  let handle_chunk ~handle_message ~pushback =
    let stop_error sexp = return (`Stop (`Protocol_error (Error.create_s sexp))) in
    let rec loop iobuf =
      let chunk_hi_bound = Iobuf.Hi_bound.window iobuf in
      match Protocol.Backend.focus_on_message iobuf with
      | Error Iobuf_too_short ->
        (match Iobuf.length iobuf > force max_message_length with
         | true -> stop_error [%message "Message too long" ~iobuf_length:(Iobuf.length iobuf : int)]
         | false ->
           let%bind () = pushback () in
           return `Continue)
      | Error (Nonsense_message_length v) ->
        stop_error [%message "Nonsense message length in header" ~_:(v : int)]
      | Error (Unknown_message_type other) ->
        stop_error [%message "Unrecognised message type character" (other : char)]
      | Ok message_type ->
        let message_hi_bound = Iobuf.Hi_bound.window iobuf in
        let res =
          let iobuf = Iobuf.read_only iobuf in
          handle_message message_type iobuf
        in
        let res =
          match res with
          | Protocol_error _ as res -> res
          | Stop _ | Continue as res ->
            match check_window_after_handle_message message_type iobuf ~message_hi_bound with
            | Error err -> Protocol_error err
            | Ok () -> res
        in
        Iobuf.bounded_flip_hi iobuf chunk_hi_bound;
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

  let run_reader ?(pushback=no_pushback) t ~handle_message =
    let handle_chunk = Staged.unstage (handle_chunk ~handle_message ~pushback) in
    match t.state with
    | Failed err ->
      return (Connection_closed err)
    | Closed_gracefully | Closing ->
      force can't_read_because_closed
    | Open ->
      (* [t.state] = [Open _] implies [t.reader] is not closed, and so this function will
         not raise. Note further that if the reader is closed _while_ we are reading, it
         does not raise. *)
      let%bind res = Reader.read_one_iobuf_at_a_time t.reader ~handle_chunk in
      (* In case of some failure (writer failure, including asynchronous) the
         reader will be closed and reads will return Eof. So, after the read
         result is determined, we check [t.state], so that we can give a better
         error message than "unexpected EOF". *)
      match t.state with
      | Failed err ->
        return (Connection_closed err)
      | Closing | Closed_gracefully ->
        force can't_read_because_closed
      | Open ->
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

  let handle_notice_response iobuf =
    match Protocol.Backend.NoticeResponse.consume iobuf with
    | Error _ as err -> err
    | Ok info ->
      Log.Global.sexp ~level:`Info [%message "Postgres NoticeResponse" (info : Info.t)];
      Ok ()

  let handle_parameter_status t iobuf =
    match Protocol.Backend.ParameterStatus.consume iobuf with
    | Error _ as err -> err
    | Ok { key; data } ->
      Hashtbl.set t.runtime_parameters ~key ~data;
      Ok ()

  let handle_notification_response t iobuf =
    match Protocol.Backend.NotificationResponse.consume iobuf with
    | Error _ as err -> err
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
      Ok ()

  let read_messages ?pushback t ~handle_message =
    let continue_if_ok =
      function
      | Error err -> Protocol_error err
      | Ok () -> Continue
    in
    let handle_message message_type iobuf =
      match (message_type : Protocol.Backend.constructor) with
      | NoticeResponse       -> continue_if_ok (handle_notice_response iobuf)
      | ParameterStatus      -> continue_if_ok (handle_parameter_status t iobuf)
      | NotificationResponse -> continue_if_ok (handle_notification_response t iobuf)
      | _ -> handle_message message_type iobuf
    in
    run_reader ?pushback t ~handle_message

  let consume_one_asynchronous_message t =
    let stop_if_ok =
      function
      | Error err -> Protocol_error err
      | Ok () -> Stop ()
    in
    let handle_message message_type iobuf =
      match (message_type : Protocol.Backend.constructor) with
      | NoticeResponse       -> stop_if_ok (handle_notice_response iobuf)
      | ParameterStatus      -> stop_if_ok (handle_parameter_status t iobuf)
      | NotificationResponse -> stop_if_ok (handle_notification_response t iobuf)
      | ErrorResponse ->
        (* ErrorResponse is a weird one, because it's very much a message that's part of
           the request-response part of the protocol, but secretly you will actually get
           one if e.g. your backend is terminated by [pg_terminate_backend]. The fact that
           this asynchronous error-response possibility isn't mentioned in the
           "protocol-flow" docs kinda doesn't matter as the connection is being destroyed.

           Ultimately handling it specially here only changes the contents of the
           [Protocol_error _] we return (i.e., behaviour is unch vs. the catch-all case).
           Think of it as giving people better error messages, not a semantic thing. *)
        let tag = "ErrorResponse received asynchronously, assuming connection is dead" in
        let error =
          match Protocol.Backend.ErrorResponse.consume iobuf with
          | Error err -> err
          | Ok err -> Error.tag err ~tag
        in
        Protocol_error error
      | other ->
        Protocol_error (Error.create_s (
          [%message
            "Unsolicited message from server outside of query conversation"
              (other : Protocol.Backend.constructor)
          ]
        ))
    in
    run_reader t ~handle_message
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

(* So there are a few different ways one could imagine handling asynchronous messages, and
   it basically breaks down to whether or not you have a single background job pulling
   messages out of the reader, filtering away asynchronous messages, and putting the
   remainder somewhere that request-response functions can pull them out of, or you have
   some way of synchronising access to the reader and noticing if it lights up while no
   request-response function is running.

   We've gone for the latter, basically because efficiently handing messages from the
   background to request-response functions is painful, and it puts a lot of complicated
   state into [t] (the background job can't be separate, because it needs to know whether
   or not [t.sequencer] is in use in order to classify an [ErrorResponse] as part of
   request-response or as asynchronous).

   The tradeoff is that we need a custom [Query_sequencer] module for the [when_idle]
   function (though at least that can be a separate, isolated module), and we need to use
   the fairly low level [interruptible_ready_to] on the [Reader]'s fd (which is skethcy).
   All in, I think this turns out to be simpler, and it also isolates the features that
   add async message support from the rest of the library. *)
let handle_asynchronous_messages t =
  Query_sequencer.when_idle t.sequencer (fun () ->
    let module Q = Query_sequencer in
    match t.state with
    | Failed _ | Closing | Closed_gracefully -> return Q.Finished
    | Open ->
      let%bind res =
        match Reader.bytes_available t.reader > 0 with
        | true -> return `Ready
        | false ->
          Fd.interruptible_ready_to
            (Reader.fd t.reader)
            `Read
            ~interrupt:(Query_sequencer.other_jobs_are_waiting t.sequencer)
      in
      match t.state with
      | Closed_gracefully | Closing | Failed _ -> return Q.Finished
      | Open ->
        match res with
        | `Interrupted ->
          return Q.Call_me_when_idle_again
        | `Bad_fd | `Closed as res ->
          (* [t.state = Open _] implies the reader is open. *)
          raise_s [%message "handle_asynchronous_messages" (res : [`Bad_fd | `Closed ])]
        | `Ready ->
          match%bind consume_one_asynchronous_message t with
          | Connection_closed _ ->
            return Q.Finished
          | Done () ->
            return Q.Call_me_when_idle_again
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
      | `Result (Done (Ok ())) ->
        handle_asynchronous_messages t;
        return (Ok t)

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
  | Closing | Closed_gracefully ->
    return (Connection_closed (
      Error.of_string "query issued against connection closed by user"))
  | Open ->
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
          match Array.length values = Array.length column_names with
          | false ->
            protocol_error_s
              [%message
                "number of columns in DataRow message did not match RowDescription"
                  (column_names : string array)
                  (values : string option array)
              ]
          | true ->
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
    Query_sequencer.enqueue t.sequencer (fun () ->
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
  Query_sequencer.enqueue t.sequencer (fun () ->
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
    Query_sequencer.enqueue t.sequencer (fun () ->
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

let listen_to_notifications t ~channel ~f =
  let query = String_escaping.Listen.query ~channel in
  let channel = Notification_channel.of_string channel in
  let bus = notification_bus t channel in
  let monitor = Monitor.current () in
  let subscriber =
    Bus.subscribe_exn
      (Bus.read_only bus)
      [%here]
      ~f:(fun payload -> f ~payload)
      ~on_callback_raise:(fun error -> Monitor.send_exn monitor (Error.to_exn error))
  in
  ignore (subscriber : _ Bus.Subscriber.t);
  query_expect_no_data t query
