open! Core
open! Async
module Protocol = Postgres_async_protocol
module Notification_channel = Protocol.Notification_channel
module Command_complete = Command_complete
module Column_metadata = Protocol.Column_metadata
module Row_handle = Row_handle
module Types = Protocol.Types
module Ssl_mode = Ssl_mode
module Pgasync_error = Pgasync_error
module Or_pgasync_error = Or_pgasync_error

let validate_max_message_length len =
  let low = Byte_units.of_kilobytes 1. in
  let high =
    match Word_size.word_size with
    | W32 -> Byte_units.of_bytes_int Sys.max_string_length
    | W64 -> Byte_units.of_bytes_int (Int.of_int32_exn Int32.max_value)
  in
  if Byte_units.between len ~low ~high
  then Byte_units.bytes_int_exn len
  else invalid_arg "invalid max_message_length"
;;

let max_message_length_override () =
  Option.try_with (fun () ->
    let s = Sys.getenv_exn "POSTGRES_ASYNC_OVERRIDE_MAX_MESSAGE_LENGTH" in
    let value =
      try Int.of_string s |> Byte_units.of_bytes_int with
      | _ -> Byte_units.of_string s
    in
    validate_max_message_length value)
;;

(* https://www.postgresql.org/docs/current/protocol.html *)

module Expert_with_command_complete = struct
  (* Unlike the [Closing]...[Closed_gracefully] sequence, when an error occurs we must
     immediately transition to the [Failed] state, to prevent any other operations being
     attempted while we are releasing resources.

     So that [closed_finished] can wait on all resources to be released in the [Failed]
     case, we have this bool [resources_released].

     The reason we don't model graceful close this way is because [Closing] can transition
     to either of [Failed _] or [Closed_gracefully] states; putting a bool in that
     constructor makes it sound like we never transition away from some hypothetical
     [Closed _] constructor. *)
  type state =
    | Open
    | Closing
    | Failed of
        { error : Pgasync_error.t
        ; resources_released : bool
        }
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

    type packed_where_to_connect =
      | Where_to_connect : 'a Tcp.Where_to_connect.t -> packed_where_to_connect

    type t = private
      { writer : Writer.t opaque
      ; reader : Reader.t
      ; mutable state : state
      ; close_started : unit Ivar.t
      ; close_finished_expert : unit Or_pgasync_error.t Ivar.t
      ; close_finished : unit Or_error.t Deferred.t
      ; sequencer : Query_sequencer.t
      ; runtime_parameters : string String.Table.t
      ; notification_buses :
          (Pid.t -> string -> unit, read_write) Bus.t Notification_channel.Table.t
      ; backend_key : Types.backend_key Set_once.t
      ; buffer_byte_limit : int
      ; max_message_length : int
      ; where_to_connect : packed_where_to_connect
      }
    [@@deriving sexp_of]

    val create_internal
      :  buffer_byte_limit:int
      -> max_message_length:int
      -> where_to_connect:packed_where_to_connect
      -> Reader.t
      -> Writer.t
      -> t

    val failed : t -> Pgasync_error.t -> unit
    val writer : t -> Writer.t
    val runtime_parameters : t -> string String.Map.t
    val reader : t -> Reader.t
    val backend_key : t -> Types.backend_key option

    type to_flush_or_not =
      | Not_required
      | Write_afterwards

    (** [flush_message] is a bit of a weird argument for this function/weird feature to
        give this part of the code, but it reminds us to always consider the need to
        flush. *)
    val catch_write_errors
      :  t
      -> f:(Writer.t -> unit)
      -> flush_message:to_flush_or_not
      -> unit

    val bytes_pending_in_writer_buffer : t -> int
    val wait_for_writer_buffer_to_be_empty : t -> unit Deferred.t
    val status : t -> state
    val close_finished : t -> unit Or_pgasync_error.t Deferred.t

    val close
      :  ?try_cancel_statement_before_close:bool
      -> t
      -> unit Or_pgasync_error.t Deferred.t

    val pq_cancel : t -> unit Or_error.t Deferred.t
  end = struct
    type 'a opaque = 'a

    type packed_where_to_connect =
      | Where_to_connect : 'a Tcp.Where_to_connect.t -> packed_where_to_connect

    type t =
      { writer : (Writer.t[@sexp.opaque])
      ; reader : (Reader.t[@sexp.opaque])
      ; mutable state : state
      ; close_started : unit Ivar.t
      ; close_finished_expert : unit Or_pgasync_error.t Ivar.t
      ; close_finished : unit Or_error.t Deferred.t
      ; sequencer : Query_sequencer.t
      ; runtime_parameters : string String.Table.t
      ; notification_buses :
          (Pid.t -> string -> unit, read_write) Bus.t Notification_channel.Table.t
      ; backend_key : Types.backend_key Set_once.t
      ; buffer_byte_limit : int
      ; max_message_length : int
      ; where_to_connect : (packed_where_to_connect[@sexp.opaque])
      }
    [@@deriving sexp_of]

    let writer t : Writer.t = t.writer
    let runtime_parameters t = String.Map.of_hashtbl_exn t.runtime_parameters
    let reader t = t.reader
    let backend_key t = Set_once.get t.backend_key

    let set_state t state =
      t.state <- state;
      match state with
      | Open -> ()
      | Failed { error; resources_released = true } ->
        Ivar.fill_if_empty t.close_started ();
        Ivar.fill_if_empty t.close_finished_expert (Error error)
      | Closed_gracefully -> Ivar.fill_if_empty t.close_finished_expert (Ok ())
      | Closing | Failed { resources_released = false; _ } ->
        Ivar.fill_if_empty t.close_started ()
    ;;

    let cleanup_resources t =
      let%bind () = Writer.close t.writer ~force_close:Deferred.unit in
      let%bind () = Reader.close t.reader in
      Hashtbl.clear t.notification_buses;
      Hashtbl.clear t.runtime_parameters;
      return ()
    ;;

    let failed t error =
      match t.state with
      | Failed _ | Closed_gracefully -> ()
      | Open | Closing ->
        set_state t (Failed { error; resources_released = false });
        don't_wait_for
          (let%bind () = cleanup_resources t in
           set_state t (Failed { error; resources_released = true });
           return ())
    ;;

    let create_internal
      ~buffer_byte_limit
      ~max_message_length
      ~where_to_connect
      reader
      writer
      =
      let close_finished_expert = Ivar.create () in
      { reader
      ; writer
      ; state = Open
      ; close_started = Ivar.create ()
      ; close_finished_expert
      ; close_finished = Ivar.read close_finished_expert >>| Or_pgasync_error.to_or_error
      ; sequencer = Query_sequencer.create ()
      ; runtime_parameters = String.Table.create ()
      ; notification_buses = Notification_channel.Table.create ~size:1 ()
      ; backend_key = Set_once.create ()
      ; buffer_byte_limit
      ; where_to_connect
      ; max_message_length
      }
    ;;

    type to_flush_or_not =
      | Not_required
      | Write_afterwards

    let catch_write_errors t ~f ~flush_message : unit =
      try
        f t.writer;
        match flush_message with
        | Not_required -> ()
        | Write_afterwards -> Protocol.Frontend.Writer.flush t.writer
      with
      | exn ->
        failed
          t
          (Pgasync_error.of_exn
             exn
             ~error_code:Pgasync_error.Sqlstate.connection_does_not_exist)
    ;;

    let bytes_pending_in_writer_buffer t = Writer.bytes_to_write t.writer
    let wait_for_writer_buffer_to_be_empty t = Writer.flushed t.writer

    (* To issue a cancel request, the frontend opens a new connection to the server and
       sends a CancelRequest message, rather than the StartupMessage message that would
       ordinarily be sent across a new connection. The server will process this request and
       then close the connection. For security reasons, no direct reply is made to the
       cancel request message.
    *)
    let pq_cancel t =
      let (Where_to_connect server) = t.where_to_connect in
      match Set_once.get t.backend_key with
      | None -> error_s [%message "No backend key found"] |> return
      | Some { pid; secret } ->
        (match%bind
           Monitor.try_with_or_error ~rest:`Log (fun () -> Tcp.connect server)
         with
         | Error _ as result -> return result
         | Ok (_sock, tcp_reader, tcp_writer) ->
           Protocol.Frontend.(
             Writer.cancel_request tcp_writer { CancelRequest.pid; secret });
           let%bind () = Writer.close tcp_writer in
           let%bind () = Reader.close tcp_reader in
           return (Ok ()))
    ;;

    let do_close_gracefully_if_possible ~try_cancel_statement_before_close t =
      match t.state with
      | Failed _ | Closed_gracefully | Closing -> return ()
      | Open ->
        set_state t Closing;
        (match try_cancel_statement_before_close with
         | false -> ()
         | true ->
           don't_wait_for
             (match%map Monitor.try_with_join_or_error (fun () -> pq_cancel t) with
              | Ok () -> ()
              | Error (_ : Error.t) -> ()));
        let closed_gracefully_deferred =
          Query_sequencer.enqueue t.sequencer (fun () ->
            catch_write_errors t ~flush_message:Not_required ~f:(fun writer ->
              Protocol.Frontend.Writer.terminate writer);
            (* we'll get an exception if the reader is already closed. *)
            match%bind
              Monitor.try_with ~run:`Now ~rest:`Log (fun () -> Reader.read_char t.reader)
            with
            | Ok `Eof -> return (Ok ())
            | Ok (`Ok c) ->
              let also_available = Reader.peek_available t.reader ~len:128 in
              return
                (Or_pgasync_error.errorf
                   ~error_code:Pgasync_error.Sqlstate.connection_exception
                   "close: EOF expected, but got '%c%s'"
                   c
                   also_available)
            | Error exn -> return (Or_pgasync_error.of_exn exn))
        in
        (match%bind Clock.with_timeout (sec 1.) closed_gracefully_deferred with
         | `Timeout ->
           failed
             t
             (Pgasync_error.of_string
                ~error_code:Pgasync_error.Sqlstate.connection_exception
                "close: EOF expected, but instead stuck");
           return ()
         | `Result (Error err) ->
           failed t err;
           return ()
         | `Result (Ok ()) ->
           let%bind () = cleanup_resources t in
           (match t.state with
            | Open | Closed_gracefully -> assert false
            | Failed _ ->
              (* e.g., if the Writer had an asynchronous exn while the reader was being
                 closed, we might have called [failed t] and filled this ivar already. *)
              return ()
            | Closing ->
              set_state t Closed_gracefully;
              return ()))
    ;;

    let status t = t.state
    let close_finished t = Ivar.read t.close_finished_expert

    let close ?(try_cancel_statement_before_close = false) t =
      don't_wait_for
        (do_close_gracefully_if_possible ~try_cancel_statement_before_close t);
      close_finished t
    ;;
  end

  include T

  let notification_bus t channel =
    Hashtbl.find_or_add t.notification_buses channel ~default:(fun () ->
      Bus.create_exn
        Arity2
        ~on_subscription_after_first_write:Allow
        ~on_callback_raise:Error.raise)
  ;;

  (* [Message_reading] hides the helper functions of [read_messages] from the below. *)
  module Message_reading : Message_reading_intf.S with type t := t = struct
    open Eager_deferred.Use

    type 'a handle_message_result =
      | Stop of 'a
      | Continue
      | Protocol_error of Pgasync_error.t

    (* [Reader.read_one_iobuf_at_a_time] hands us 'chunks' to [handle_chunk]. A chunk is
       an iobuf, where the window of the iobuf is set to the data that has been pulled
       from the OS into the [Reader.t] but not yet consumed by the application. Said
       window 'chunk' contains messages, potentially a partial message at the end in case
       e.g. the bytes of a message is split across two calls to the [read] syscall.

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
       payload, i.e., the bytes 'AAAAA', i.e., this iobuf but with the window set to
       [5,10). [Protocol.Backend.focus_on_message] does this by consuming the header
       'hhhhh' to learn the type and length (thereby moving the window lo-bound up) and
       then resizing it (moving the hi-bound down) like so:

       {v
          hhhhhAAAAAhhhhhBBBBBhhh
              /-wdw-\
       v}

       Now we can call [handle_message] with this iobuf.

       The contract we have with [handle_message] is that it will use calls to functions
       in [Iobuf.Consume] to consume the bytes inside the window we set, that is, consume
       the entire message. As it consumes bytes of the message, the window lo-bound is
       moved up, so when it's done consuming the window is [10,10)

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

       then, we recurse (focus on [15,20), call [handle_message], etc.), and recurse
       again.

       At this point, reading the 'message length' field tells us that we only have part
       of 'C' in our buffer (three out of 5+5 bytes). So, we return from [handle_chunk].
       The contract we have with [Reader.t] is that we will leave in the iobuf the data
       that we did not consume. It looks at the iobuf window to understand what's left,
       keeps only those three bytes of 'C' in its internal buffer, and uses [read] to
       refill the buffer before calling us again; hopefully at that point we'll have the
       whole of message 'C'.

       For this to work it is important that [handle_message] actually consumed the whole
       message, and did not move the window hi-bound. We _could_ save those bounds and
       forcibly restore them before doing the flip, however we prefer to instead just
       check that [handle_message] did this because the functions that parse messages are
       written to 'consume' them anyway, and checking that they actually did so is a good
       validation that we correctly understand and are thinking about all fields in the
       postgres protocol; not consuming all the bytes of the message indicates a potential
       parsing bug. That's what [check_window_after_handle_message] does.

       Note the check against [max_message_length]. Without it, if postgres told us that
       it was sending us an extremely long message, we'd return from [handle_chunk]
       constantly asking for refills, always thinking we needed more data and consuming
       all the RAM. Instead, we will just bail out rather than asking for refills if a
       message gets too long. The limit is arbitrary (1GB), selected to match current
       column and row length limits in postgres, but ought to be enough for anybody (with
       an environment variable escape hatch if we turn out to be wrong). *)

    let check_window_after_handle_message message_type iobuf ~message_hi_bound =
      match
        [%compare.equal: Iobuf.Hi_bound.t] (Iobuf.Hi_bound.window iobuf) message_hi_bound
      with
      | false ->
        Or_pgasync_error.error_s
          [%message
            "handle_message moved the hi-bound!"
              (message_type : Protocol.Backend.constructor)]
      | true ->
        (match Iobuf.is_empty iobuf with
         | false ->
           Or_pgasync_error.error_s
             [%message
               "handle_message did not consume entire iobuf"
                 (message_type : Protocol.Backend.constructor)
                 ~bytes_remaining:(Iobuf.length iobuf : int)]
         | true -> Ok ())
    ;;

    let handle_chunk t ~handle_message =
      let stop_error sexp =
        return
          (`Stop
            (`Protocol_error
              (Pgasync_error.create_s
                 ~error_code:Pgasync_error.Sqlstate.protocol_violation
                 sexp)))
      in
      let rec loop iobuf =
        let chunk_hi_bound = Iobuf.Hi_bound.window iobuf in
        match Protocol.Backend.focus_on_message iobuf with
        | Error Iobuf_too_short_for_header -> return `Continue
        | Error (Iobuf_too_short_for_message { message_length }) ->
          (match message_length > t.max_message_length with
           | true -> stop_error [%message "Message too long" (message_length : int)]
           | false -> return `Continue)
        | Error (Nonsense_message_length v) ->
          stop_error [%message "Nonsense message length in header" ~_:(v : int)]
        | Error (Unknown_message_type other) ->
          stop_error [%message "Unrecognised message type character" (other : char)]
        | Ok message_type ->
          let message_hi_bound = Iobuf.Hi_bound.window iobuf in
          let%bind.Eager_deferred res =
            let iobuf = Iobuf.read_only iobuf in
            handle_message message_type iobuf
          in
          let res =
            match res with
            | Protocol_error _ as res -> res
            | (Stop _ | Continue) as res ->
              (match
                 check_window_after_handle_message message_type iobuf ~message_hi_bound
               with
               | Error err -> Protocol_error err
               | Ok () -> res)
          in
          Iobuf.bounded_flip_hi iobuf chunk_hi_bound;
          (match res with
           | Protocol_error err -> return (`Stop (`Protocol_error err))
           | Continue -> loop iobuf
           | Stop s -> return (`Stop (`Done s)))
      in
      Staged.stage loop
    ;;

    type 'a read_messages_result =
      | Connection_closed of Pgasync_error.t
      | Done of 'a

    let can't_read_because_closed_and_eof =
      lazy
        (Connection_closed
           (Pgasync_error.of_string
              ~error_code:Pgasync_error.Sqlstate.connection_does_not_exist
              "can't read from connection: connection is either closed or closing, and \
               no messages available"))
    ;;

    let run_reader t ~handle_message =
      let handle_chunk = Staged.unstage (handle_chunk t ~handle_message) in
      let bytes_available =
        if Reader.is_closed t.reader then false else Reader.bytes_available t.reader > 0
      in
      (* If there are still bytes available in the reader, we would try to consume them,
         even if our internal state indicates that the connection is closed. We would
         never have an excessive amount of data to process, both due to the postgres write
         behaviour and because of input buffer limits *)
      match t.state, bytes_available with
      | Failed { error; _ }, _ -> return (Connection_closed error)
      | (Closed_gracefully | Closing), false ->
        return (force can't_read_because_closed_and_eof)
      | Open, _ | (Closed_gracefully | Closing), true ->
        (* [t.state] = [Open _] implies [t.reader] is not closed, and so this function will
           not raise. Note further that if the reader is closed _while_ we are reading, it
           does not raise. *)
        let on_protocol_error err =
          failed t err;
          Connection_closed err
        in
        (match%map Reader.read_one_iobuf_at_a_time t.reader ~handle_chunk with
         | `Stopped (`Protocol_error error) -> on_protocol_error error
         | `Stopped (`Done res) -> Done res
         | (`Eof_with_unconsumed_data _ | `Eof) as res ->
           (* In case of some failure (writer failure, including asynchronous) the reader
               will be closed and reads will return Eof. So, after the read result is
               determined, we check [t.state], so that we can give a better error message
               than "unexpected EOF". *)
           (match t.state with
            | Failed { error; _ } -> Connection_closed error
            | Closing | Closed_gracefully -> force can't_read_because_closed_and_eof
            | Open ->
              (match res with
               | `Eof_with_unconsumed_data data ->
                 on_protocol_error
                   (Pgasync_error.create_s
                      ~error_code:Pgasync_error.Sqlstate.connection_does_not_exist
                      [%message
                        "Unexpected EOF" ~unconsumed_bytes:(String.length data : int)])
               | `Eof ->
                 on_protocol_error
                   (Pgasync_error.of_string
                      ~error_code:Pgasync_error.Sqlstate.connection_does_not_exist
                      "Unexpected EOF (no unconsumed messages)"))))
    ;;

    let handle_notice_response iobuf =
      match Protocol.Backend.NoticeResponse.consume iobuf with
      | Error err -> Error (Pgasync_error.of_error err)
      | Ok { error_code = _; all_fields } ->
        let all_fields =
          if am_running_test
          then
            (* Make output more stable accross different server versions *)
            List.filter all_fields ~f:(fun (field, _value) ->
              not
                Protocol.Backend.Error_or_notice_field.(
                  equal field Line || equal field File || equal field Routine))
          else all_fields
        in
        [%log.global.info
          "Postgres NoticeResponse"
            ~_:(all_fields : (Protocol.Backend.Error_or_notice_field.t * string) list)];
        Ok ()
    ;;

    let handle_parameter_status t iobuf =
      match Protocol.Backend.ParameterStatus.consume iobuf with
      | Error err ->
        Error
          (Pgasync_error.of_error
             ~error_code:Pgasync_error.Sqlstate.protocol_violation
             err)
      | Ok { key; data } ->
        Hashtbl.set t.runtime_parameters ~key ~data;
        Ok ()
    ;;

    let handle_notification_response t iobuf =
      match Protocol.Backend.NotificationResponse.consume iobuf with
      | Error err ->
        Error
          (Pgasync_error.of_error
             ~error_code:Pgasync_error.Sqlstate.protocol_violation
             err)
      | Ok { pid; channel; payload } ->
        let bus = notification_bus t channel in
        (match Bus.num_subscribers bus with
         | 0 ->
           [%log.global.error
             "Postgres NotificationResponse on channel that no callbacks are listening to"
               (channel : Notification_channel.t)]
         | _ -> Bus.write2 bus pid payload);
        Ok ()
    ;;

    let read_messages' t ~handle_message =
      let[@inline] continue_if_ok = function
        | Error err -> return (Protocol_error err)
        | Ok () -> return Continue
      in
      run_reader t ~handle_message:(fun message_type iobuf ->
        match (message_type : Protocol.Backend.constructor) with
        | NoticeResponse -> continue_if_ok (handle_notice_response iobuf)
        | ParameterStatus -> continue_if_ok (handle_parameter_status t iobuf)
        | NotificationResponse -> continue_if_ok (handle_notification_response t iobuf)
        | _ -> handle_message message_type iobuf)
    ;;

    let read_messages ?pushback t ~handle_message =
      read_messages' t ~handle_message:(fun message_type iobuf ->
        let result = handle_message message_type iobuf in
        match pushback with
        | None -> return result
        | Some pushback ->
          let%map () = pushback () in
          result)
    ;;

    let consume_one_asynchronous_message t =
      let stop_if_ok = function
        | Error err -> return (Protocol_error err)
        | Ok () -> return (Stop ())
      in
      let handle_message message_type iobuf =
        match (message_type : Protocol.Backend.constructor) with
        | NoticeResponse -> stop_if_ok (handle_notice_response iobuf)
        | ParameterStatus -> stop_if_ok (handle_parameter_status t iobuf)
        | NotificationResponse -> stop_if_ok (handle_notification_response t iobuf)
        | ErrorResponse ->
          (* ErrorResponse is a weird one, because it's very much a message that's part of
             the request-response part of the protocol, but secretly you will actually get
             one if e.g. your backend is terminated by [pg_terminate_backend]. The fact
             that this asynchronous error-response possibility isn't mentioned in the
             "protocol-flow" docs kinda doesn't matter as the connection is being
             destroyed.

             Ultimately handling it specially here only changes the contents of the
             [Protocol_error _] we return (i.e., behaviour is unch vs. the catch-all
             case). Think of it as giving people better error messages, not a semantic
             thing. *)
          let tag =
            "ErrorResponse received asynchronously, assuming connection is dead"
          in
          let error =
            match Protocol.Backend.ErrorResponse.consume iobuf with
            | Error err ->
              Pgasync_error.of_error
                err
                ~error_code:Pgasync_error.Sqlstate.protocol_violation
            | Ok err -> Pgasync_error.of_error_response err |> Pgasync_error.tag ~tag
          in
          return (Protocol_error error)
        | other ->
          return
            (Protocol_error
               (Pgasync_error.create_s
                  ~error_code:Pgasync_error.Sqlstate.protocol_violation
                  [%message
                    "Unsolicited message from server outside of query conversation"
                      (other : Protocol.Backend.constructor)]))
      in
      run_reader t ~handle_message
    ;;
  end

  include Message_reading

  let protocol_error_of_error error =
    let error_code = Pgasync_error.Sqlstate.protocol_violation in
    Protocol_error (Pgasync_error.of_error ~error_code error)
  ;;

  let protocol_error_s sexp =
    let error_code = Pgasync_error.Sqlstate.protocol_violation in
    Protocol_error (Pgasync_error.create_s ~error_code sexp)
  ;;

  let unexpected_msg_type ~here msg_type state =
    protocol_error_s
      [%message
        "Unexpected message type"
          (msg_type : Protocol.Backend.constructor)
          (state : Sexp.t)
          (here : Source_code_position.t)]
  ;;

  let login t ~startup_message ~password ~gss_krb_token =
    let user = Protocol.Frontend.StartupMessage.user startup_message in
    catch_write_errors t ~flush_message:Not_required ~f:(fun writer ->
      Protocol.Frontend.Writer.startup_message writer startup_message);
    read_messages t ~handle_message:(fun msg_type iobuf ->
      match msg_type with
      | AuthenticationRequest ->
        let module Q = Protocol.Backend.AuthenticationRequest in
        (match Q.consume iobuf with
         | Error err -> protocol_error_of_error err
         | Ok Ok -> Continue
         | Ok (MD5Password { salt }) ->
           (match password with
            | Some password ->
              let d s = Md5.to_hex (Md5.digest_string s) in
              let md5_hex = "md5" ^ d (d (password ^ user) ^ salt) in
              catch_write_errors t ~flush_message:Not_required ~f:(fun writer ->
                Protocol.Frontend.Writer.password_message
                  writer
                  (Cleartext_or_md5_hex md5_hex));
              Continue
            | None ->
              let s = "Server requested (md5) password, but no password was provided" in
              Stop
                (Or_pgasync_error.error_string
                   ~error_code:Pgasync_error.Sqlstate.invalid_password
                   s))
         | Ok GSS ->
           (match gss_krb_token with
            | Some token ->
              catch_write_errors t ~flush_message:Not_required ~f:(fun writer ->
                Protocol.Frontend.Writer.password_message writer (Gss_binary_blob token));
              Continue
            | None ->
              let s = "Server requested GSS auth, but no gss_krb_token was provided" in
              Stop
                (Or_pgasync_error.error_string
                   ~error_code:Pgasync_error.Sqlstate.invalid_password
                   s))
         | Ok other ->
           let s =
             sprintf !"Server wants unimplemented auth subtype: %{sexp:Q.t}" other
           in
           Stop
             (Or_pgasync_error.error_string
                ~error_code:Pgasync_error.Sqlstate.invalid_authorization_specification
                s))
      | BackendKeyData ->
        (match Protocol.Backend.BackendKeyData.consume iobuf with
         | Error err -> protocol_error_of_error err
         | Ok data ->
           (match Set_once.set t.backend_key data with
            | Ok () -> Continue
            | Error _ -> protocol_error_s [%sexp "duplicate BackendKeyData messages"]))
      | ReadyForQuery ->
        let module Q = Protocol.Backend.ReadyForQuery in
        (match Q.consume iobuf with
         | Error err -> protocol_error_of_error err
         | Ok Idle -> Stop (Ok ())
         | Ok st ->
           protocol_error_s [%message "Unexpected initial transaction status" (st : Q.t)])
      | ErrorResponse ->
        (match Protocol.Backend.ErrorResponse.consume iobuf with
         | Error err -> protocol_error_of_error err
         | Ok err -> Stop (Error (Pgasync_error.of_error_response err)))
      | msg_type -> unexpected_msg_type ~here:[%here] msg_type [%sexp "logging in"])
  ;;

  (* So there are a few different ways one could imagine handling asynchronous messages,
     and it basically breaks down to whether or not you have a single background job
     pulling messages out of the reader, filtering away asynchronous messages, and putting
     the remainder somewhere that request-response functions can pull them out of, or you
     have some way of synchronising access to the reader and noticing if it lights up
     while no request-response function is running.

     We've gone for the latter, basically because efficiently handing messages from the
     background to request-response functions is painful, and it puts a lot of complicated
     state into [t] (the background job can't be separate, because it needs to know
     whether or not [t.sequencer] is in use in order to classify an [ErrorResponse] as
     part of request-response or as asynchronous).

     The tradeoff is that we need a custom [Query_sequencer] module for the [when_idle]
     function (though at least that can be a separate, isolated module), and we need to
     use the fairly low level [interruptible_ready_to] on the [Reader]'s fd (which is
     skethcy). All in, I think this turns out to be simpler, and it also isolates the
     features that add async message support from the rest of the library. *)
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
        (match t.state with
         | Closed_gracefully | Closing | Failed _ -> return Q.Finished
         | Open ->
           (match res with
            | `Interrupted -> return Q.Call_me_when_idle_again
            | (`Bad_fd | `Closed) as res ->
              (* [t.state = Open _] implies the reader is open. *)
              raise_s
                [%message "handle_asynchronous_messages" (res : [ `Bad_fd | `Closed ])]
            | `Ready ->
              (match%bind consume_one_asynchronous_message t with
               | Connection_closed _ -> return Q.Finished
               | Done () -> return Q.Call_me_when_idle_again))))
  ;;

  let maybe_ssl_wrap
    ~buffer_byte_limit
    ~interrupt
    ~where_to_connect
    ~max_message_length
    ~ssl_mode
    ~tcp_reader
    ~tcp_writer
    =
    let negotiate_ssl ~required =
      match Protocol.Frontend.Writer.ssl_request tcp_writer () with
      | exception exn -> return (Or_error.of_exn exn)
      | () ->
        (match%bind
           choose
             [ choice interrupt (fun () -> `Interrupt)
             ; choice (Reader.read_char tcp_reader) (function
                 | `Eof -> `Eof
                 | `Ok char -> `Ok char)
             ]
         with
         | `Interrupt -> return (error_s [%message "ssl negotiation interrupted"])
         | `Eof ->
           return (error_s [%message "Reader closed before ssl negotiation response"])
         | `Ok 'S' ->
           (* [Prefer] and [Require] do not demand certificate verification, which is
              why you see us assign [Verify_none] and a [verify_callback] that
              unconditionally returns [Ok ()]. We'd need to revisit that if we added
              [Ssl_mode.t] constructors akin to libpq's [Verify_ca] or
              [Verify_full]. *)
           Monitor.try_with_or_error ~rest:`Log (fun () ->
             let%bind t, (_ : [ `Connection_closed of unit Deferred.t ]) =
               (* Async_ssl (and its use of Writer.pipe) internally propagates pushback
                  between the readers and writers, so it should have a relatively small
                  number of bytes stuck in its internal buffers.

                  Applying [buffer_byte_limit] to [create_internal] suffices. *)
               Async_ssl.Tls.Expert.wrap_client_connection_and_stay_open
                 (Async_ssl.Config.Client.create
                    ~verify_modes:[ Verify_none ]
                    ~ca_file:None
                    ~ca_path:None
                    ~remote_hostname:None
                    ~verify_callback:(fun (_ : Async_ssl.Ssl.Connection.t) ->
                      return (Ok ()))
                    ())
                 tcp_reader
                 tcp_writer
                 ~f:(fun (_ : Async_ssl.Ssl.Connection.t) ssl_reader ssl_writer ->
                   let t =
                     create_internal
                       ~where_to_connect
                       ~buffer_byte_limit
                       ~max_message_length
                       ssl_reader
                       ssl_writer
                   in
                   let close_finished_deferred =
                     let%bind (_ : unit Or_pgasync_error.t) = close_finished t in
                     return ()
                   in
                   return (t, `Do_not_close_until close_finished_deferred))
             in
             return t)
         | `Ok 'N' ->
           (match required with
            | true ->
              return
                (error_s
                   [%message
                     "Server indicated it cannot use SSL connections, but ssl_mode is \
                      set to Require"])
            | false ->
              (* 'N' means the server is unwilling to use SSL connections, but is happy to
                 proceed without encryption (and sending a [StartupMessage] on the same
                 connection is the correct way to do so). *)
              return
                (Ok
                   (create_internal
                      ~where_to_connect
                      ~buffer_byte_limit
                      ~max_message_length
                      tcp_reader
                      tcp_writer)))
         | `Ok response_char ->
           (* 'E' (or potentially another character?) can indicate that the server
              doesn't understand the request, and we must re-start the connection from
              scratch.

              This should only happen for ancient versions of postgres, which are not
              supported by this library. *)
           return
             (error_s
                [%message
                  "Postgres Server indicated it does not understand the SSLRequest \
                   message. This may mean that the server is running a very outdated \
                   version of postgres, or some other problem may be occurring. You can \
                   try to run with ssl_mode = Disable to skip the SSLRequest and use \
                   plain TCP."
                    (response_char : Char.t)]))
    in
    match (ssl_mode : Ssl_mode.t) with
    | Disable ->
      let t =
        create_internal
          ~where_to_connect
          ~max_message_length
          ~buffer_byte_limit
          tcp_reader
          tcp_writer
      in
      return (Ok t)
    | Prefer -> negotiate_ssl ~required:false
    | Require -> negotiate_ssl ~required:true
  ;;

  let connect_tcp_and_maybe_start_ssl
    ~buffer_age_limit
    ~buffer_byte_limit
    ~max_message_length
    ~interrupt
    ~ssl_mode
    where_to_connect
    =
    let (Where_to_connect server) = where_to_connect in
    match%bind
      Monitor.try_with_or_error ~rest:`Log (fun () ->
        Tcp.connect ~buffer_age_limit ~interrupt server)
    with
    | Error _ as result -> return result
    | Ok (_sock, tcp_reader, tcp_writer) ->
      (match%bind
         maybe_ssl_wrap
           ~where_to_connect
           ~buffer_byte_limit
           ~max_message_length
           ~interrupt
           ~ssl_mode
           ~tcp_reader
           ~tcp_writer
       with
       | Error _ as result ->
         let%bind () = Writer.close tcp_writer in
         let%bind () = Reader.close tcp_reader in
         return result
       | Ok t ->
         let writer_failed =
           Monitor.detach_and_get_next_error (Writer.monitor tcp_writer)
         in
         return (Ok (t, writer_failed)))
  ;;

  let create_and_login
    ?(interrupt = Deferred.never ())
    ?(ssl_mode = Ssl_mode.Disable)
    ?server
    ?password
    ?gss_krb_token
    ?(buffer_age_limit = `Unlimited)
    ?(buffer_byte_limit = Byte_units.of_megabytes 128.)
    ?(max_message_length = Byte_units.of_gigabytes 1.)
    ~startup_message
    ()
    =
    match%bind
      Deferred.Or_error.try_with_join ~extract_exn:true (fun () ->
        let max_message_length =
          match max_message_length_override () with
          | Some override -> override
          | None -> validate_max_message_length max_message_length
        in
        let where_to_connect =
          match server with
          | Some x -> Where_to_connect x
          | None ->
            Where_to_connect
              (Tcp.Where_to_connect.of_file "/run/postgresql/.s.PGSQL.5432")
        in
        connect_tcp_and_maybe_start_ssl
          ~buffer_age_limit
          ~buffer_byte_limit:(Byte_units.bytes_int_exn buffer_byte_limit)
          ~max_message_length
          ~interrupt
          ~ssl_mode
          where_to_connect)
    with
    | Error error ->
      return
        (Error
           (Pgasync_error.of_error
              ~error_code:
                Pgasync_error.Sqlstate.sqlclient_unable_to_establish_sqlconnection
              error))
    | Ok (t, writer_failed) ->
      upon writer_failed (fun exn ->
        failed
          t
          (Pgasync_error.create_s
             ~error_code:Pgasync_error.Sqlstate.connection_does_not_exist
             [%message "Writer failed asynchronously" (exn : Exn.t)]));
      let login_failed err =
        failed t err;
        return (Error err)
      in
      (match%bind
         choose
           [ choice interrupt (fun () -> `Interrupt)
           ; choice (login t ~startup_message ~password ~gss_krb_token) (fun l ->
               `Result l)
           ]
       with
       | `Interrupt ->
         login_failed
           (Pgasync_error.of_string
              ~error_code:
                Pgasync_error.Sqlstate.sqlclient_unable_to_establish_sqlconnection
              "login interrupted")
       | `Result (Connection_closed err) -> return (Error err)
       | `Result (Done (Error err)) -> login_failed err
       | `Result (Done (Ok ())) -> return (Ok t))
  ;;

  let connect
    ?interrupt
    ?ssl_mode
    ?server
    ?user
    ?password
    ?gss_krb_token
    ?buffer_age_limit
    ?buffer_byte_limit
    ?max_message_length
    ~database
    ?replication
    ()
    =
    let%bind.Deferred.Result startup_message =
      (* we default to the values in [parameters] *)
      (let%bind.Deferred.Result user =
         match user with
         | Some u -> return (Ok u)
         | None -> Monitor.try_with_or_error Unix.username ~extract_exn:true
       in
       return
         (Or_error.try_with (fun () ->
            Protocol.Frontend.StartupMessage.create_exn ~user ~database ?replication ())))
      >>| Result.Error.map
            ~f:
              (Pgasync_error.of_error
                 ~error_code:
                   Pgasync_error.Sqlstate.sqlclient_unable_to_establish_sqlconnection)
    in
    let%map.Deferred.Result t =
      create_and_login
        ?interrupt
        ?ssl_mode
        ?server
        ?password
        ?gss_krb_token
        ?buffer_age_limit
        ?buffer_byte_limit
        ?max_message_length
        ~startup_message
        ()
    in
    handle_asynchronous_messages t;
    t
  ;;

  let with_connection
    ?interrupt
    ?ssl_mode
    ?server
    ?user
    ?password
    ?gss_krb_token
    ?buffer_age_limit
    ?buffer_byte_limit
    ?max_message_length
    ?try_cancel_statement_before_close
    ~database
    ?replication
    ~on_handler_exception:`Raise
    func
    =
    let%bind.Deferred.Result t =
      connect
        ?interrupt
        ?ssl_mode
        ?server
        ?user
        ?password
        ?gss_krb_token
        ?buffer_age_limit
        ?buffer_byte_limit
        ?max_message_length
        ~database
        ?replication
        ()
    in
    match%bind Monitor.try_with ~run:`Now ~rest:`Raise (fun () -> func t) with
    | Ok _ as ok_res ->
      let%bind (_ : unit Or_pgasync_error.t) =
        close ?try_cancel_statement_before_close t
      in
      return ok_res
    | Error exn ->
      don't_wait_for
        (let%bind (_ : unit Or_pgasync_error.t) =
           close ?try_cancel_statement_before_close t
         in
         return ());
      raise exn
  ;;

  (* We use the extended query protocol rather than the simple query protocol because it
     provides support for [parameters] (via the 'bind' message), and because it guarantees
     that only a SQL statement is executed per query, which simplifies the state machine
     we need to use to handle the responses significantly.

     We don't currently take advantage of named statements or portals, we just use the
     'unnamed' ones. We don't take advantage of the ability to bind a statement twice or
     partially execute a portal; we just send the full parse-bind-describe-execute
     sequence unconditionally in one go.

     Ultimately, one needs to read the "message flow/extended query" section of the
     documentation:

     https://www.postgresql.org/docs/10/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY

     What follows is a brief summary, and then a description of how the different
     functions fit in.

     When any error occurs, the server will report the error and tehen discard messages
     until a 'sync' message. This means that we can safely send
     parse-bind-describe-execute in one go and then read all the responses, because if one
     step fails the remainder will be ignored. So, we do this because it improves latency.

     [parse_and_start_executing_query] instructs the server to parse the query, bind the
     parameters, describe the type of the rows that will be produced. It then reads
     response messages, walking through a state machine. The first three states are easy,
     because we expect either the relevant response messages (ParseComplete, BindComplete,
     RowDescription|NoData), or an ErrorResponse.

     After that, things are trickier. We don't know what string was in the query and
     whether or not the query emits no data (e.g., a DELETE), or if server is going to
     send us rows, or if it's going to try and COPY IN/OUT, etc.

     [parse_and_start_executing_query] detects which case we're in and returns a value of
     type [setup_loop_result] indicating this. It's a little weird because we don't want
     to consume any of the actual data (if applicable) in that function, just detect the
     mode.

     If the response to the [Describe] message was a [RowDescription], we know that the
     response to the [Execute] will be some [DataRow]s, so we don't look at any messages
     after that (i.e., in that case we don't look at the response to [Execute] in this
     function).

     However if the response to [Describe] was [NoData], then we _have_ to look at the
     first message in response to the [Execute] message to know which case we're in.
     _Fortunately_, the first message in all the other cases does not actually contain any
     data, so we can do what we want (detect mode and return).

     Here they are:

     - [About_to_deliver_rows] e.g., a SELECT. We know we're in this case iff the response
       to our [Describe] message was a [RowDescription], and we don't look at the response
       to the [Execute] message; the caller is responsible for consuming the [DataRow]s.
     - [About_to_copy_out] i.e., COPY ... TO STDOUT. We're here iff we get
       a [CopyOutResponse] in response to our [Execute].
     - [Ready_to_copy_in] i.e., COPY ... FROM STDIN. We're here iff we get
       a [CopyInResponse]. We return, and the caller is responsible for sending a load of
       [CopyData] messages etc.
     - [EmptyQuery] i.e., the query was the empty string. We're here if we get
       a [EmptyQueryResponse], at which point we're done (and can sync); we do not expect
       a [CommandComplete].
     - [Command_complete_without_output], e.g. a DELETE. We're here if we just get
       [CommandComplete] straight away.

     Note that it's also possible for the [About_to_deliver_rows] case to complete without
     output. The difference is that one corresponds to receiving [RowDescription],
     [CommandComplete] and the other [NoData], [CommandComplete]. *)
  type setup_loop_state =
    | Parsing
    | Binding
    | Describing
    | Executing
  [@@deriving sexp_of]

  type setup_loop_result =
    | About_to_deliver_rows of Protocol.Backend.RowDescription.t
    | About_to_copy_out
    | Ready_to_copy_in of Protocol.Backend.CopyInResponse.t
    | Empty_query
    | Command_complete_without_output of Command_complete.t
    | Remote_reported_error of Pgasync_error.t

  let parse_command_complete tag =
    (* If there is a row count, it is always the last space-separated word in the tag, see
       https://www.postgresql.org/docs/current/protocol-message-formats.html and pgjdbc
       implementation (CommandCompleteParser.java)

       Tag would be one of:
       - COMMAND OID ROWS
       - COMMAND ROWS

       we remove both OID and ROWS from the tag, and throw OID away as it is always 0 after
       pg11 (where WITH OIDS was removed).
    *)
    match String.rsplit2 tag ~on:' ' with
    | None ->
      (* "BEGIN", "ROLLBACK", etc *)
      Command_complete.create ~tag ~rows:None
    | Some (tag_and_maybe_oid, maybe_rows) ->
      (match Int.of_string_opt maybe_rows with
       | None ->
         (* "CREATE TABLE", "CREATE INDEX", ... *)
         Command_complete.create ~tag ~rows:None
       | Some _ as rows ->
         (match String.rsplit2 tag_and_maybe_oid ~on:' ' with
          | Some (tag, "0") ->
            (* INSERT 0 123 *)
            Command_complete.create ~tag ~rows
          | _ ->
            (* COPY 123 *)
            Command_complete.create ~tag:tag_and_maybe_oid ~rows))
  ;;

  let parse_and_start_executing_query t query_string ~parameters =
    match t.state with
    | Failed { error; _ } ->
      (* See comment at top of file regarding erasing error codes from this error. *)
      return
        (Connection_closed
           (Pgasync_error.create_s
              ~error_code:Pgasync_error.Sqlstate.connection_does_not_exist
              [%message
                "query issued against previously-failed connection"
                  ~original_error:(error : Pgasync_error.t)]))
    | Closing | Closed_gracefully ->
      return
        (Connection_closed
           (Pgasync_error.of_string
              ~error_code:Pgasync_error.Sqlstate.connection_does_not_exist
              "query issued against connection closed by user"))
    | Open ->
      catch_write_errors t ~flush_message:Not_required ~f:(fun writer ->
        (* This is modeled after PQsendQueryGuts from libpq/fe-exec.c *)
        Protocol.Frontend.Writer.parse
          writer
          { destination = Types.Statement_name.unnamed; query = query_string };
        (* There is no need to explicitly close this portal later, as per
           https://www.postgresql.org/docs/current/protocol-flow.html:

           "If successfully created, a named portal object lasts till the end of the
           current transaction, unless explicitly destroyed. An unnamed portal is
           destroyed at the end of the transaction, or as soon as the next Bind statement
           specifying the unnamed portal as destination is issued. (Note that a simple
           Query message also destroys the unnamed portal.) Named portals must be
           explicitly closed before they can be redefined by another Bind message, but
           this is not required for the unnamed portal."
        *)
        Protocol.Frontend.Writer.bind
          writer
          { destination = Types.Portal_name.unnamed
          ; statement = Types.Statement_name.unnamed
          ; parameters
          };
        Protocol.Frontend.Writer.describe writer (Portal Types.Portal_name.unnamed);
        Protocol.Frontend.Writer.execute
          writer
          { portal = Types.Portal_name.unnamed; limit = Unlimited };
        (* We used to issue Flush instead of Sync here, but CockroachDB treats Flush as an
           indication that we are we are batching statements (see "Batched Statements" at
           https://www.cockroachlabs.com/docs/stable/transactions) and freaks out when you
           send something like "SET CLUSTER SETTING".

           Since we never actually do batching - API does not provide for this - we now
           Sync instead, like libpq does *)
        Protocol.Frontend.Writer.sync writer);
      let state = ref Parsing in
      let unexpected_msg_type ~here msg_type =
        unexpected_msg_type ~here msg_type [%sexp (state : setup_loop_state ref)]
      in
      read_messages t ~handle_message:(fun msg_type iobuf ->
        match !state, msg_type with
        | state, ErrorResponse ->
          (match Protocol.Backend.ErrorResponse.consume iobuf with
           | Error err -> protocol_error_of_error err
           | Ok err ->
             let tag =
               sprintf !"Postgres Server Error (state=%{sexp:setup_loop_state})" state
             in
             let err = Pgasync_error.of_error_response err |> Pgasync_error.tag ~tag in
             Stop (Remote_reported_error err))
        | Parsing, ParseComplete ->
          let () = Protocol.Backend.ParseComplete.consume iobuf in
          state := Binding;
          Continue
        | Parsing, msg_type -> unexpected_msg_type ~here:[%here] msg_type
        | Binding, BindComplete ->
          let () = Protocol.Backend.BindComplete.consume iobuf in
          state := Describing;
          Continue
        | Binding, msg_type -> unexpected_msg_type ~here:[%here] msg_type
        | Describing, RowDescription ->
          (match Protocol.Backend.RowDescription.consume iobuf with
           | Error err -> protocol_error_of_error err
           | Ok description -> Stop (About_to_deliver_rows description))
        | Describing, NoData ->
          let () = Protocol.Backend.NoData.consume iobuf in
          state := Executing;
          Continue
        | Describing, msg_type -> unexpected_msg_type ~here:[%here] msg_type
        | Executing, EmptyQueryResponse ->
          let () = Protocol.Backend.EmptyQueryResponse.consume iobuf in
          Stop Empty_query
        | Executing, CommandComplete ->
          (match Protocol.Backend.CommandComplete.consume iobuf with
           | Error err -> protocol_error_of_error err
           | Ok str -> Stop (Command_complete_without_output (parse_command_complete str)))
        | Executing, CopyInResponse ->
          (match Protocol.Backend.CopyInResponse.consume iobuf with
           | Error err -> protocol_error_of_error err
           | Ok details -> Stop (Ready_to_copy_in details))
        | Executing, CopyOutResponse ->
          (match Protocol.Backend.CopyOutResponse.consume iobuf with
           | Error err -> protocol_error_of_error err
           | Ok _ -> Stop About_to_copy_out)
        | Executing, CopyBothResponse ->
          (* CopyBothResponse is only used for streaming replication, which we do not
             initiate. *)
          unexpected_msg_type ~here:[%here] msg_type
        | Executing, msg_type -> unexpected_msg_type ~here:[%here] msg_type)
  ;;

  (* really the return type of [f] should be [Protocol_error _ | Continue], but it's
     convenient to re-use [handle_message_result] and use [Nothing.t] to 'delete' the
     third constructor... *)
  let read_datarows t ~pushback ~f =
    read_messages t ?pushback ~handle_message:(fun msg_type iobuf ->
      match msg_type with
      | DataRow ->
        (match f ~datarow_iobuf:iobuf with
         | (Protocol_error _ | Continue) as x -> x
         | Stop (_ : Nothing.t) -> .)
      | ErrorResponse ->
        (match Protocol.Backend.ErrorResponse.consume iobuf with
         | Error err -> protocol_error_of_error err
         | Ok err ->
           let err =
             Pgasync_error.of_error_response err
             |> Pgasync_error.tag ~tag:"Error during query execution (despite parsing ok)"
           in
           Stop (Error err))
      | CommandComplete ->
        (match Protocol.Backend.CommandComplete.consume iobuf with
         | Error err -> protocol_error_of_error err
         | Ok str -> Stop (Ok (parse_command_complete str)))
      | msg_type -> unexpected_msg_type ~here:[%here] msg_type [%sexp "reading DataRows"])
  ;;

  let drain_datarows t =
    let count = ref 0 in
    let f ~datarow_iobuf =
      incr count;
      Protocol.Backend.DataRow.skip datarow_iobuf;
      Continue
    in
    match%bind read_datarows t ~pushback:None ~f with
    | Connection_closed _ as e -> return e
    | Done (Ok command_complete) -> return (Done (!count, command_complete))
    | Done (Error _) -> return (Done (!count, Command_complete.empty))
  ;;

  let drain_copy_out t =
    let seen_copy_done = ref false in
    read_messages t ~handle_message:(fun msg_type iobuf ->
      match msg_type with
      | ErrorResponse ->
        (* [ErrorResponse] terminates copy-out mode; no separate [CopyDone] is required. *)
        (match Protocol.Backend.ErrorResponse.consume iobuf with
         | Error err -> protocol_error_of_error err
         | Ok _ -> Stop ())
      | CopyData ->
        (match !seen_copy_done with
         | true -> protocol_error_s [%sexp "CopyData message after CopyDone?"]
         | false ->
           Protocol.Shared.CopyData.skip iobuf;
           Continue)
      | CopyDone ->
        Protocol.Shared.CopyDone.consume iobuf;
        seen_copy_done := true;
        Continue
      | CommandComplete ->
        (match Protocol.Backend.CommandComplete.consume iobuf with
         | Error err -> protocol_error_of_error err
         | Ok (_ : string) ->
           (match !seen_copy_done with
            | false -> protocol_error_s [%sexp "CommandComplete before CopyDone?"]
            | true -> Stop ()))
      | msg_type ->
        unexpected_msg_type ~here:[%here] msg_type [%sexp "draining copy-out mode"])
  ;;

  let abort_copy_in t ~reason =
    catch_write_errors t ~flush_message:Not_required ~f:(fun writer ->
      Protocol.Frontend.Writer.copy_fail writer { reason };
      Protocol.Frontend.Writer.sync writer);
    read_messages t ~handle_message:(fun msg_type iobuf ->
      match msg_type with
      | ErrorResponse ->
        (match Protocol.Backend.ErrorResponse.consume iobuf with
         | Error err -> protocol_error_of_error err
         | Ok _ -> Stop ())
      | msg_type ->
        unexpected_msg_type ~here:[%here] msg_type [%sexp "aborting copy-in mode"])
  ;;

  let consume_ready_for_query t =
    read_messages t ~handle_message:(fun msg_type iobuf ->
      match msg_type with
      | ReadyForQuery ->
        (match Protocol.Backend.ReadyForQuery.consume iobuf with
         | Error err -> protocol_error_of_error err
         | Ok (Idle | In_transaction | In_failed_transaction) -> Stop (Ok ()))
      | ErrorResponse ->
        (match Protocol.Backend.ErrorResponse.consume iobuf with
         | Error err -> protocol_error_of_error err
         | Ok err ->
           let err =
             Pgasync_error.of_error_response err
             |> Pgasync_error.tag
                  ~tag:"Error waiting for ReadyForQuery (after query execution finished)"
           in
           Stop (Error err))
      | msg_type ->
        unexpected_msg_type ~here:[%here] msg_type [%sexp "waiting for ReadyForQuery"])
  ;;

  let handle_ready_for_query t query_result =
    match query_result with
    | Connection_closed err -> return (Error err)
    | Done (Error query_err) ->
      (match%bind consume_ready_for_query t with
       | Connection_closed (_ : Pgasync_error.t)
       | Done (Error (_ : Pgasync_error.t))
       | Done (Ok ()) -> return (Error query_err))
    | Done (Ok result) ->
      (* Error here could indicate an implicit transaction failed to commit *)
      (match%bind consume_ready_for_query t with
       | Connection_closed err -> return (Error err)
       | Done (Error rfq_error) -> return (Error rfq_error)
       | Done (Ok ()) -> return (Ok result))
  ;;

  let internal_query_gen
    t
    ?(parameters = [||])
    ?pushback
    ~handle_columns
    query_string
    ~prepare
    ~handle_row
    =
    let%bind result =
      match%bind parse_and_start_executing_query t query_string ~parameters with
      | Connection_closed _ as err -> return err
      | Done About_to_copy_out ->
        let%bind (Connection_closed _ | Done ()) = drain_copy_out t in
        return
          (Done
             (Or_pgasync_error.error_s
                [%message "COPY TO STDOUT is not appropriate for [Postgres_async.query]"]))
      | Done (Ready_to_copy_in _) ->
        let reason = "COPY FROM STDIN is not appropriate for [Postgres_async.query]" in
        let%bind (Connection_closed _ | Done ()) = abort_copy_in t ~reason in
        return (Done (Or_pgasync_error.error_string reason))
      | Done (Remote_reported_error error) -> return (Done (Error error))
      | Done Empty_query -> return (Done (Ok Command_complete.empty))
      | Done (Command_complete_without_output command_complete) ->
        return (Done (Ok command_complete))
      | Done (About_to_deliver_rows description) ->
        handle_columns description;
        let init = prepare ~about_to_deliver_rows:description in
        read_datarows t ~pushback ~f:(fun ~datarow_iobuf ->
          handle_row init ~description ~datarow_iobuf)
    in
    handle_ready_for_query t result
  ;;

  let internal_query_zero_copy
    t
    ?parameters
    ?pushback
    ~handle_columns
    query_string
    ~(handle_row : Row_handle.t -> unit)
    =
    internal_query_gen
      t
      ?parameters
      ?pushback
      ~handle_columns
      query_string
      ~prepare:(fun ~about_to_deliver_rows:_ -> ())
      ~handle_row:(fun () ~description ~datarow_iobuf ->
        match
          handle_row (Row_handle.Private.create description ~datarow:datarow_iobuf)
        with
        | () -> Continue
        | exception exn ->
          protocol_error_s [%sexp "Failed to parse DataRow", (exn : Exn.t)])
  ;;

  let internal_query t ?parameters ?pushback ~handle_columns query_string ~handle_row =
    internal_query_gen
      t
      ?parameters
      ?pushback
      ~handle_columns
      query_string
      ~prepare:(fun ~about_to_deliver_rows ->
        Iarray.map about_to_deliver_rows ~f:Column_metadata.name)
      ~handle_row:(fun column_names ~description:_ ~datarow_iobuf:iobuf ->
        match Protocol.Backend.DataRow.consume iobuf with
        | Error err -> protocol_error_of_error err
        | Ok values ->
          (match Iarray.length values = Iarray.length column_names with
           | false ->
             protocol_error_s
               [%message
                 "number of columns in DataRow message did not match RowDescription"
                   (column_names : string iarray)
                   (values : string option iarray)]
           | true ->
             handle_row ~column_names ~values;
             Continue))
  ;;

  module Callback_wrapper = struct
    type t = { mutable callback_raised : bool }

    let create () = { callback_raised = false }

    let wrap1_local t ~f arg =
      match t.callback_raised with
      | true -> ()
      | false ->
        (match f arg with
         | () -> ()
         | exception exn ->
           (* it's important that we drain (and discard) the remaining rows. *)
           Monitor.send_exn (Monitor.current ()) exn;
           t.callback_raised <- true)
    ;;

    let wrap1 t ~f arg =
      wrap1_local
        t
        ~f:(fun ({ global } : _ Modes.Global.t) -> f global)
        ({ global = arg } : _ Modes.Global.t) [@nontail]
    ;;

    let complete t ~result ~parameters ~query_string =
      match t.callback_raised with
      | true -> Deferred.never ()
      | false ->
        return
          (Result.map_error
             ~f:(Pgasync_error.tag_by_query ?parameters ~query_string)
             result)
    ;;
  end

  (* [query] wraps [internal_query], acquiring the sequencer lock and keeping the user's
     exceptions away from trashing our state. *)
  let query t ?parameters ?pushback ?handle_columns query_string ~handle_row =
    let callback_wrapper = Callback_wrapper.create () in
    let handle_columns description =
      match handle_columns with
      | None -> ()
      | Some handler -> Callback_wrapper.wrap1 callback_wrapper ~f:handler description
    in
    let handle_row ~column_names ~values =
      Callback_wrapper.wrap1
        callback_wrapper
        ~f:(fun () -> handle_row ~column_names ~values)
        () [@nontail]
    in
    let%bind result =
      Query_sequencer.enqueue t.sequencer (fun () ->
        internal_query t ?parameters ?pushback ~handle_columns query_string ~handle_row)
    in
    Callback_wrapper.complete callback_wrapper ~result ~query_string ~parameters
  ;;

  let query_zero_copy t ?parameters ?pushback ?handle_columns query_string ~handle_row =
    let callback_wrapper = Callback_wrapper.create () in
    let handle_columns description =
      match handle_columns with
      | None -> ()
      | Some handler -> Callback_wrapper.wrap1 callback_wrapper ~f:handler description
    in
    let%bind result =
      Query_sequencer.enqueue t.sequencer (fun () ->
        internal_query_zero_copy
          t
          ?parameters
          ?pushback
          ~handle_columns
          query_string
          ~handle_row:(fun row ->
            Callback_wrapper.wrap1_local callback_wrapper ~f:handle_row row))
    in
    Callback_wrapper.complete callback_wrapper ~result ~query_string ~parameters
  ;;

  (* Type to hold execution status of chain of simple query commands *)
  module Simple_query_status = struct
    type state =
      | Success of Command_complete.t list
      | Unsupported_message_type of Pgasync_error.t
      | Remote_reported_error of Pgasync_error.t

    type t =
      { result : state
      ; warnings : Error.t list
      }
    [@@deriving fields ~getters ~iterators:create]

    let update_result t result = { t with result }
    let add_warning t warning = { t with warnings = warning :: t.warnings }

    let add_command_complete t command_complete =
      match t.result with
      | Unsupported_message_type _ | Remote_reported_error _ -> t
      | Success results -> { t with result = Success (results @ [ command_complete ]) }
    ;;
  end

  module Simple_query_result = struct
    type t =
      | Completed_with_no_warnings of Command_complete.t list
      | Completed_with_warnings of (Command_complete.t list * Error.t list)
      | Failed of Pgasync_error.t
      | Connection_error of Pgasync_error.t
      | Driver_error of Pgasync_error.t

    let to_or_pgasync_error = function
      | Completed_with_no_warnings command_complete
      | Completed_with_warnings (command_complete, (_ : Error.t list)) ->
        Ok command_complete
      | Failed error | Connection_error error | Driver_error error -> Result.fail error
    ;;

    let map_error t ~f =
      match t with
      | Completed_with_no_warnings _ | Completed_with_warnings _ -> t
      | Failed error -> Failed (f error)
      | Connection_error error -> Connection_error (f error)
      | Driver_error error -> Driver_error (f error)
    ;;
  end

  (* Runs through the state-machine for the messages returned from the server
     as a result of a simple query. Returns whether the query was successfully
     executed or, if not, an appropriate error message. *)
  let handle_simple_query_messages ?pushback t ~handle_columns ~handle_row =
    let current_row_description = ref None in
    let status =
      ref (Simple_query_status.Fields.create ~result:(Success []) ~warnings:[])
    in
    read_messages ?pushback t ~handle_message:(fun msg_type iobuf ->
      match (msg_type : Protocol.Backend.constructor) with
      | CommandComplete ->
        (* A RowDescription can be followed by multiple DataRows, and then
           a CommandComplete. Only reset [current_row_description] when we know
           all DataRows have been received. *)
        current_row_description := None;
        (match Protocol.Backend.CommandComplete.consume iobuf with
         | Error err -> protocol_error_of_error err
         | Ok command_complete ->
           status
           := Simple_query_status.add_command_complete
                !status
                (parse_command_complete command_complete);
           Continue)
      | ReadyForQuery ->
        (match Protocol.Backend.ReadyForQuery.consume iobuf with
         | Error err -> protocol_error_of_error err
         | Ok (_ : Protocol.Backend.ReadyForQuery.t) -> Stop !status)
      | RowDescription ->
        (match Protocol.Backend.RowDescription.consume iobuf with
         | Error err -> protocol_error_of_error err
         | Ok (row_description : Protocol.Backend.RowDescription.t) ->
           handle_columns row_description;
           current_row_description := Some row_description;
           Continue)
      | DataRow ->
        (match !current_row_description, Protocol.Backend.DataRow.consume iobuf with
         | None, (_ : Protocol.Backend.DataRow.t Or_error.t) ->
           protocol_error_s [%message "Non-existent Row Description for Data Row."]
         | Some (_ : Protocol.Backend.RowDescription.t), Error err ->
           protocol_error_of_error err
         | Some row_description, Ok values ->
           let column_names = Iarray.map row_description ~f:Column_metadata.name in
           (match Iarray.length column_names <> Iarray.length values with
            | true ->
              protocol_error_s
                [%message
                  "number of columns in DataRow message did not match RowDescription"
                    (column_names : string iarray)
                    (values : string option iarray)]
            | false ->
              handle_row ~column_names ~values;
              Continue))
      | EmptyQueryResponse ->
        Protocol.Backend.EmptyQueryResponse.consume iobuf;
        Continue
      | ErrorResponse ->
        (match Protocol.Backend.ErrorResponse.consume iobuf with
         | Error err -> protocol_error_of_error err
         | Ok error_response ->
           let tag = "Postgres Server Error" in
           let error_response =
             Pgasync_error.of_error_response error_response |> Pgasync_error.tag ~tag
           in
           (* When ErrorResponse is sent, it is immediately followed by a
              ReadyForQuery since the rest of the query is terminated. Save
              this code and then return it in the future.*)
           status
           := Simple_query_status.update_result
                !status
                (Remote_reported_error error_response);
           Continue)
      | CopyInResponse ->
        (match Protocol.Backend.CopyInResponse.consume iobuf with
         | Error err -> protocol_error_of_error err
         | Ok (_ : Protocol.Backend.CopyInResponse.t) ->
           let reason =
             "Command ignored: COPY FROM STDIN is not appropriate for \
              [Postgres_async.simple_query]"
           in
           (* Abort COPY IN command on the server side. An ErrorResponse
              will be returned by the server following this which will
              terminate the query. *)
           catch_write_errors t ~flush_message:Not_required ~f:(fun writer ->
             Protocol.Frontend.Writer.copy_fail writer { reason });
           let warning = Error.create_s [%message reason] in
           status := Simple_query_status.add_warning !status warning;
           Continue)
      | CopyOutResponse ->
        (match Protocol.Backend.CopyOutResponse.consume iobuf with
         | Error err -> protocol_error_of_error err
         | Ok (_ : Protocol.Backend.CopyOutResponse.t) ->
           (* Store the error messsage and read remaining messages for
              COPY OUT *)
           let warning =
             Error.create_s
               [%message
                 "Command ignored: COPY TO STDOUT is not appropriate for \
                  [Postgres_async.simple_query]"]
           in
           status := Simple_query_status.add_warning !status warning;
           Continue)
      | CopyData ->
        Protocol.Shared.CopyData.skip iobuf;
        Continue
      | CopyDone ->
        Protocol.Shared.CopyDone.consume iobuf;
        Continue
      (* Note that NoticeResponse is handled directly in [read_messages] so we do not need
         a match statement for it in this function. *)
      | message_type ->
        let new_result =
          Simple_query_status.Unsupported_message_type
            (Pgasync_error.create_s
               ~error_code:Pgasync_error.Sqlstate.protocol_violation
               [%message
                 "Invalid message type for Postgres Simple Query protocol"
                   (message_type : Protocol.Backend.constructor)])
        in
        status := Simple_query_status.update_result !status new_result;
        Stop !status)
  ;;

  let simple_query ?pushback ?handle_columns t query_string ~handle_row =
    let callback_raised = ref false in
    let wrap_callback ~f =
      match !callback_raised with
      | true -> ()
      | false ->
        (match f () with
         | () -> ()
         | exception exn ->
           (* it's important that we drain (and discard) the remaining rows. *)
           Monitor.send_exn (Monitor.current ()) exn;
           callback_raised := true)
    in
    let handle_row ~column_names ~values =
      wrap_callback ~f:(fun () -> handle_row ~column_names ~values)
    in
    let handle_columns description =
      match handle_columns with
      | None -> ()
      | Some handler -> wrap_callback ~f:(fun () -> handler description)
    in
    let%bind result =
      Query_sequencer.enqueue t.sequencer (fun () ->
        catch_write_errors t ~flush_message:Write_afterwards ~f:(fun writer ->
          Protocol.Frontend.Writer.query writer query_string);
        let%map return_status =
          handle_simple_query_messages ?pushback t ~handle_columns ~handle_row
        in
        match return_status with
        | Connection_closed error -> Simple_query_result.Connection_error error
        | Done return_status ->
          (match Simple_query_status.result return_status with
           | Unsupported_message_type error -> Simple_query_result.Driver_error error
           | Remote_reported_error error -> Simple_query_result.Failed error
           | Success command_complete ->
             (match Simple_query_status.warnings return_status with
              | [] -> Simple_query_result.Completed_with_no_warnings command_complete
              | warnings ->
                Simple_query_result.Completed_with_warnings (command_complete, warnings))))
    in
    match !callback_raised with
    | true -> Deferred.never ()
    | false ->
      return
        (Simple_query_result.map_error
           ~f:(Pgasync_error.tag_by_query ~query_string)
           result)
  ;;

  let execute_simple t query_string =
    let returned_rows = ref false in
    let%map result =
      simple_query
        t
        query_string
        ~handle_row:
          (fun
            ~column_names:(_ : string iarray) ~values:(_ : string option iarray) ->
          returned_rows := true)
    in
    if !returned_rows
    then
      Simple_query_result.Driver_error
        (Pgasync_error.create_s
           [%message "[Postgres_async.execute_simple]: query returned at least one row"])
    else result
  ;;

  (* [query_expect_no_data] doesn't need the separation that [internal_query] and [query]
     have because there's no callback to wrap and it's easy enough to just throw the
     sequencer around the whole function. *)
  let query_expect_no_data t ?(parameters = [||]) query_string =
    Query_sequencer.enqueue t.sequencer (fun () ->
      let%bind result =
        match%bind parse_and_start_executing_query t query_string ~parameters with
        | Connection_closed _ as err -> return err
        | Done About_to_copy_out ->
          let%bind (Connection_closed _ | Done ()) = drain_copy_out t in
          return
            (Done
               (Or_pgasync_error.error_s
                  [%message
                    "[Postgres_async.query_expect_no_data]: query attempted COPY OUT"]))
        | Done (Ready_to_copy_in _) ->
          let reason = "[Postgres_async.query_expect_no_data]: query attempted COPY IN" in
          let%bind (Connection_closed _ | Done ()) = abort_copy_in t ~reason in
          return (Done (Or_pgasync_error.error_string reason))
        | Done (About_to_deliver_rows _) ->
          (match%bind drain_datarows t with
           | Connection_closed _ as err -> return err
           | Done (0, command_complete) -> return (Done (Ok command_complete))
           | Done _ ->
             return
               (Done
                  (Or_pgasync_error.error_s [%message "query unexpectedly produced rows"])))
        | Done (Remote_reported_error error) -> return (Done (Error error))
        | Done Empty_query -> return (Done (Ok Command_complete.empty))
        | Done (Command_complete_without_output command_complete) ->
          return (Done (Ok command_complete))
      in
      match%bind handle_ready_for_query t result with
      | Error err ->
        return (Error (Pgasync_error.tag_by_query ~parameters ~query_string err))
      | Ok result -> return (Ok result))
  ;;

  let iter_copy_out t ~query_string ~f =
    Query_sequencer.enqueue t.sequencer (fun () ->
      let%bind result =
        match%bind parse_and_start_executing_query t query_string ~parameters:[||] with
        | Connection_closed _ as err -> return err
        | Done (Ready_to_copy_in _) ->
          let reason = "[Postgres_async.copy_out]: query attempted COPY IN" in
          let%bind (Connection_closed _ | Done ()) = abort_copy_in t ~reason in
          return (Done (Or_pgasync_error.error_string reason))
        | Done (About_to_deliver_rows _) ->
          (match%bind drain_datarows t with
           | Connection_closed _ as err -> return err
           | Done (0, command_complete) -> return (Done (Ok command_complete))
           | Done _ ->
             return
               (Done
                  (Or_pgasync_error.error_s [%message "query unexpectedly produced rows"])))
        | Done (Remote_reported_error error) -> return (Done (Error error))
        | Done Empty_query -> return (Done (Ok Command_complete.empty))
        | Done (Command_complete_without_output command_complete) ->
          return (Done (Ok command_complete))
        | Done About_to_copy_out ->
          (* See https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-COPY.
             [read_messages'] handles NoticeResponse and ParameterStatus for us.
          *)
          let seen_copy_done = ref false in
          read_messages' t ~handle_message:(fun msg_type iobuf ->
            match msg_type with
            | ErrorResponse ->
              (* [ErrorResponse] terminates copy-out mode; no separate [CopyDone] is required. *)
              return
                (match Protocol.Backend.ErrorResponse.consume iobuf with
                 | Error err -> protocol_error_of_error err
                 | Ok error_response ->
                   Stop (Error (Pgasync_error.of_error_response error_response)))
            | CopyData ->
              (match !seen_copy_done with
               | true ->
                 return
                   (protocol_error_s
                      [%sexp "Received unexpected CopyData message after CopyDone"])
               | false ->
                 let%map.Eager_deferred () = f iobuf in
                 Continue)
            | CopyDone ->
              Protocol.Shared.CopyDone.consume iobuf;
              seen_copy_done := true;
              return Continue
            | CommandComplete ->
              return
                (match Protocol.Backend.CommandComplete.consume iobuf with
                 | Error err -> protocol_error_of_error err
                 | Ok tag ->
                   (match !seen_copy_done with
                    | false ->
                      protocol_error_s
                        [%sexp "Received unexpected CommandComplete before CopyDone"]
                    | true -> Stop (Ok (Command_complete.create ~tag ~rows:None))))
            | msg_type ->
              return (unexpected_msg_type ~here:[%here] msg_type [%sexp "copy-out mode"]))
      in
      match%bind handle_ready_for_query t result with
      | Ok result -> return (Ok result)
      | Error err -> return (Error (Pgasync_error.tag_by_query ~query_string err)))
  ;;

  type 'a feed_data_result =
    | Abort of { reason : string }
    | Wait of unit Deferred.t
    | Data of 'a
    | Finished

  let query_did_not_initiate_copy_in =
    lazy
      (return
         (Done (Or_pgasync_error.error_s [%message "Query did not initiate copy-in mode"])))
  ;;

  (* [internal_copy_in_raw] is to [copy_in_raw] as [internal_query] is to [query].
     Sequencer and exception handling. *)
  let internal_copy_in_raw t ?(parameters = [||]) query_string ~feed_data =
    let%bind result =
      match%bind parse_and_start_executing_query t query_string ~parameters with
      | Connection_closed _ as err -> return err
      | Done About_to_copy_out ->
        let%bind (Connection_closed _ | Done ()) = drain_copy_out t in
        return
          (Done
             (Or_pgasync_error.error_s
                [%message "COPY TO STDOUT is not appropriate for [Postgres_async.query]"]))
      | Done (Empty_query | Command_complete_without_output _) ->
        force query_did_not_initiate_copy_in
      | Done (About_to_deliver_rows _) ->
        let%bind (Connection_closed _ | Done (_ : int * Command_complete.t)) =
          drain_datarows t
        in
        force query_did_not_initiate_copy_in
      | Done (Remote_reported_error error) -> return (Done (Error error))
      | Done (Ready_to_copy_in _) ->
        let sent_copy_done = ref false in
        let (response_deferred
              : Command_complete.t Or_pgasync_error.t read_messages_result Deferred.t)
          =
          read_messages t ~handle_message:(fun msg_type iobuf ->
            match msg_type with
            | ErrorResponse ->
              (match Protocol.Backend.ErrorResponse.consume iobuf with
               | Error err -> protocol_error_of_error err
               | Ok err -> Stop (Error (Pgasync_error.of_error_response err)))
            | CommandComplete ->
              (match Protocol.Backend.CommandComplete.consume iobuf with
               | Error err -> protocol_error_of_error err
               | Ok (res : string) ->
                 (match !sent_copy_done with
                  | false ->
                    protocol_error_s
                      [%sexp "CommandComplete response before we sent CopyDone?"]
                  | true -> Stop (Ok (parse_command_complete res))))
            | msg_type -> unexpected_msg_type ~here:[%here] msg_type [%sexp "copying in"])
        in
        let%bind () =
          let rec loop () =
            match feed_data () with
            | Abort { reason } ->
              catch_write_errors t ~flush_message:Not_required ~f:(fun writer ->
                Protocol.Frontend.Writer.copy_fail writer { reason };
                Protocol.Frontend.Writer.sync writer);
              return ()
            | Wait deferred -> continue ~after:deferred
            | Data payload ->
              catch_write_errors t ~flush_message:Not_required ~f:(fun writer ->
                Protocol.Frontend.Writer.copy_data writer payload);
              (match bytes_pending_in_writer_buffer t > t.buffer_byte_limit with
               | false -> loop ()
               | true -> continue ~after:(wait_for_writer_buffer_to_be_empty t))
            | Finished ->
              sent_copy_done := true;
              catch_write_errors t ~flush_message:Not_required ~f:(fun writer ->
                Protocol.Frontend.Writer.copy_done writer;
                Protocol.Frontend.Writer.sync writer);
              return ()
          and continue ~after =
            let%bind () = after in
            (* check for early termination; not necessary for correctness, just nice. *)
            match Deferred.peek response_deferred with
            | None -> loop ()
            | Some (Connection_closed _) -> return ()
            | Some (Done (Error _)) ->
              (* "In the event of a backend-detected error during copy-in mode (including
                 receipt of a CopyFail message), the backend will issue an ErrorResponse
                 message. If the COPY command was issued via an extended-query message,
                 the backend will now discard frontend messages until a Sync message is
                 received, then it will issue ReadyForQuery and return to normal
                 processing."

                 We issue Sync here and consume ReadyForQuery in the subsequent call to
                 handle_ready_for_query *)
              catch_write_errors t ~flush_message:Not_required ~f:(fun writer ->
                Protocol.Frontend.Writer.sync writer);
              return ()
            | Some (Done (Ok (_result : Command_complete.t))) ->
              failwith
                "BUG: response_deferred is (Done (Ok result)), but we never set \
                 !sent_copy_done?"
          in
          loop ()
        in
        response_deferred
    in
    match%bind handle_ready_for_query t result with
    | Error err ->
      return (Error (Pgasync_error.tag_by_query ~parameters ~query_string err))
    | Ok result -> return (Ok result)
  ;;

  let copy_in_raw t ?parameters query_string ~feed_data =
    let callback_raised = ref false in
    let feed_data () =
      (* We shouldn't be called again after [Abort] *)
      assert (not !callback_raised);
      match feed_data () with
      | (Abort _ | Wait _ | Data _ | Finished) as x -> x
      | exception exn ->
        Monitor.send_exn (Monitor.current ()) exn;
        callback_raised := true;
        Abort { reason = "feed_data callback raised" }
    in
    let%bind result =
      Query_sequencer.enqueue t.sequencer (fun () ->
        internal_copy_in_raw t ?parameters query_string ~feed_data)
    in
    match !callback_raised with
    | true -> Deferred.never ()
    | false -> return result
  ;;

  (* [copy_in_rows] builds on [copy_in_raw] and clearly doesn't raise exceptions in the
     callback wrapper, so we can just rely on [copy_in_raw] to handle sequencing and
     exceptions here. *)
  let copy_in_rows ?schema_name t ~table_name ~column_names ~feed_data =
    let query_string =
      String_escaping.Copy_in.query ?schema_name ~table_name ~column_names ()
    in
    copy_in_raw t query_string ~feed_data:(fun () ->
      match feed_data () with
      | (Abort _ | Wait _ | Finished) as x -> x
      | Data row -> Data (String_escaping.Copy_in.row_to_string row))
  ;;

  let listen_to_notifications t ~channel ~f =
    let query = String_escaping.Listen.query ~channel in
    let channel = Notification_channel.of_string channel in
    let bus = notification_bus t channel in
    let monitor = Monitor.current () in
    let subscriber =
      Bus.subscribe_exn
        bus
        ~f:(fun pid payload -> f ~pid ~payload)
        ~on_callback_raise:(fun error -> Monitor.send_exn monitor (Error.to_exn error))
    in
    ignore (subscriber : _ Bus.Subscriber.t);
    query_expect_no_data t query
  ;;
end

module With_command_complete = struct
  type t = Expert_with_command_complete.t [@@deriving sexp_of]

  let connect
    ?interrupt
    ?ssl_mode
    ?server
    ?user
    ?password
    ?gss_krb_token
    ?buffer_age_limit
    ?buffer_byte_limit
    ?max_message_length
    ~database
    ?replication
    ()
    =
    Expert_with_command_complete.connect
      ?interrupt
      ?ssl_mode
      ?server
      ?user
      ?password
      ?gss_krb_token
      ?buffer_age_limit
      ?buffer_byte_limit
      ?max_message_length
      ~database
      ?replication
      ()
    >>| Or_pgasync_error.to_or_error
  ;;

  let close ?try_cancel_statement_before_close t =
    Expert_with_command_complete.close ?try_cancel_statement_before_close t
    >>| Or_pgasync_error.to_or_error
  ;;

  let close_finished t = (t : Expert_with_command_complete.t).close_finished

  type state =
    | Open
    | Closing
    | Failed of
        { error : Error.t
        ; resources_released : bool
        }
    | Closed_gracefully
  [@@deriving sexp_of]

  let status t =
    match Expert_with_command_complete.status t with
    | Failed { error; resources_released } ->
      Failed { error = Pgasync_error.to_error error; resources_released }
    | Open -> Open
    | Closing -> Closing
    | Closed_gracefully -> Closed_gracefully
  ;;

  let with_connection
    ?interrupt
    ?ssl_mode
    ?server
    ?user
    ?password
    ?gss_krb_token
    ?buffer_age_limit
    ?buffer_byte_limit
    ?max_message_length
    ?try_cancel_statement_before_close
    ~database
    ?replication
    ~on_handler_exception:`Raise
    func
    =
    Expert_with_command_complete.with_connection
      ?interrupt
      ?ssl_mode
      ?server
      ?user
      ?password
      ?gss_krb_token
      ?buffer_age_limit
      ?buffer_byte_limit
      ?max_message_length
      ?try_cancel_statement_before_close
      ~database
      ?replication
      ~on_handler_exception:`Raise
      func
    >>| Or_pgasync_error.to_or_error
  ;;

  type 'a feed_data_result = 'a Expert_with_command_complete.feed_data_result =
    | Abort of { reason : string }
    | Wait of unit Deferred.t
    | Data of 'a
    | Finished

  let copy_in_raw t ?parameters query_string ~feed_data =
    Expert_with_command_complete.copy_in_raw t ?parameters query_string ~feed_data
    >>| Or_pgasync_error.to_or_error
  ;;

  let copy_in_rows ?schema_name t ~table_name ~column_names ~feed_data =
    Expert_with_command_complete.copy_in_rows
      ?schema_name
      t
      ~table_name
      ~column_names
      ~feed_data
    >>| Or_pgasync_error.to_or_error
  ;;

  let listen_to_notifications t ~channel ~f =
    Expert_with_command_complete.listen_to_notifications t ~channel ~f
    >>| Or_pgasync_error.to_or_error
  ;;

  let query_expect_no_data t ?parameters query_string =
    Expert_with_command_complete.query_expect_no_data t ?parameters query_string
    >>| Or_pgasync_error.to_or_error
  ;;

  let query t ?parameters ?pushback ?handle_columns query_string ~handle_row =
    Expert_with_command_complete.query
      t
      ?parameters
      ?pushback
      ?handle_columns
      query_string
      ~handle_row
    >>| Or_pgasync_error.to_or_error
  ;;

  let query_zero_copy t ?parameters ?pushback ?handle_columns query_string ~handle_row =
    Expert_with_command_complete.query_zero_copy
      t
      ?parameters
      ?pushback
      ?handle_columns
      query_string
      ~handle_row
    >>| Or_pgasync_error.to_or_error
  ;;
end

module Expert = struct
  include Expert_with_command_complete

  let copy_in_raw t ?parameters query_string ~feed_data =
    Expert_with_command_complete.copy_in_raw t ?parameters query_string ~feed_data
    >>| Result.map ~f:(const ())
  ;;

  let copy_in_rows ?schema_name t ~table_name ~column_names ~feed_data =
    Expert_with_command_complete.copy_in_rows
      ?schema_name
      t
      ~table_name
      ~column_names
      ~feed_data
    >>| Result.map ~f:(const ())
  ;;

  let listen_to_notifications t ~channel ~f =
    Expert_with_command_complete.listen_to_notifications t ~channel ~f
    >>| Result.map ~f:(const ())
  ;;

  let query_expect_no_data t ?parameters query_string =
    Expert_with_command_complete.query_expect_no_data t ?parameters query_string
    >>| Result.map ~f:(const ())
  ;;

  let query t ?parameters ?pushback ?handle_columns query_string ~handle_row =
    Expert_with_command_complete.query
      t
      ?parameters
      ?pushback
      ?handle_columns
      query_string
      ~handle_row
    >>| Result.map ~f:(const ())
  ;;

  let query_zero_copy t ?parameters ?pushback ?handle_columns query_string ~handle_row =
    Expert_with_command_complete.query_zero_copy
      t
      ?parameters
      ?pushback
      ?handle_columns
      query_string
      ~handle_row
    >>| Result.map ~f:(const ())
  ;;
end

type t = Expert.t [@@deriving sexp_of]

let connect
  ?interrupt
  ?ssl_mode
  ?server
  ?user
  ?password
  ?gss_krb_token
  ?buffer_age_limit
  ?buffer_byte_limit
  ?max_message_length
  ~database
  ?replication
  ()
  =
  Expert.connect
    ?interrupt
    ?ssl_mode
    ?server
    ?user
    ?password
    ?gss_krb_token
    ?buffer_age_limit
    ?buffer_byte_limit
    ?max_message_length
    ~database
    ?replication
    ()
  >>| Or_pgasync_error.to_or_error
;;

let close ?try_cancel_statement_before_close t =
  Expert.close ?try_cancel_statement_before_close t >>| Or_pgasync_error.to_or_error
;;

let close_finished t = (t : Expert.t).close_finished

type state =
  | Open
  | Closing
  | Failed of
      { error : Error.t
      ; resources_released : bool
      }
  | Closed_gracefully
[@@deriving sexp_of]

let status t =
  match Expert.status t with
  | Failed { error; resources_released } ->
    Failed { error = Pgasync_error.to_error error; resources_released }
  | Open -> Open
  | Closing -> Closing
  | Closed_gracefully -> Closed_gracefully
;;

let with_connection
  ?interrupt
  ?ssl_mode
  ?server
  ?user
  ?password
  ?gss_krb_token
  ?buffer_age_limit
  ?buffer_byte_limit
  ?max_message_length
  ?try_cancel_statement_before_close
  ~database
  ?replication
  ~on_handler_exception:`Raise
  func
  =
  Expert.with_connection
    ?interrupt
    ?ssl_mode
    ?server
    ?user
    ?password
    ?gss_krb_token
    ?buffer_age_limit
    ?buffer_byte_limit
    ?max_message_length
    ?try_cancel_statement_before_close
    ~database
    ?replication
    ~on_handler_exception:`Raise
    func
  >>| Or_pgasync_error.to_or_error
;;

type 'a feed_data_result = 'a Expert.feed_data_result =
  | Abort of { reason : string }
  | Wait of unit Deferred.t
  | Data of 'a
  | Finished

let copy_in_raw t ?parameters query_string ~feed_data =
  Expert.copy_in_raw t ?parameters query_string ~feed_data
  >>| Or_pgasync_error.to_or_error
;;

let copy_in_rows ?schema_name t ~table_name ~column_names ~feed_data =
  Expert.copy_in_rows ?schema_name t ~table_name ~column_names ~feed_data
  >>| Or_pgasync_error.to_or_error
;;

let listen_to_notifications t ~channel ~f =
  Expert.listen_to_notifications t ~channel ~f >>| Or_pgasync_error.to_or_error
;;

let query_expect_no_data t ?parameters query_string =
  Expert.query_expect_no_data t ?parameters query_string >>| Or_pgasync_error.to_or_error
;;

let query t ?parameters ?pushback ?handle_columns query_string ~handle_row =
  Expert.query t ?parameters ?pushback ?handle_columns query_string ~handle_row
  >>| Result.map ~f:(const ())
  >>| Or_pgasync_error.to_or_error
;;

let query_zero_copy t ?parameters ?pushback ?handle_columns query_string ~handle_row =
  Expert.query_zero_copy t ?parameters ?pushback ?handle_columns query_string ~handle_row
  >>| Result.map ~f:(const ())
  >>| Or_pgasync_error.to_or_error
;;

module Private = struct
  module Protocol = Protocol
  module Types = Protocol.Types
  module Message_reading = Expert.Message_reading
  module Query_sequencer = Query_sequencer
  module Pgasync_error = Pgasync_error
  module Row_handle = Row_handle

  let pgasync_error_of_error = Pgasync_error.of_error ?error_code:None
  let pq_cancel = Expert.pq_cancel
  let failed = Expert.failed
  let iter_copy_out = Expert.iter_copy_out

  module Simple_query_result = Expert.Simple_query_result

  let simple_query = Expert.simple_query
  let execute_simple = Expert.execute_simple
  let writer = Expert.writer
  let runtime_parameters = Expert.runtime_parameters
  let query_sequencer (t : Expert.t) = t.sequencer
  let close_started (t : Expert.t) = Ivar.read t.close_started

  module Without_background_asynchronous_message_handling = struct
    type nonrec t = t

    let login_and_get_raw = Expert.create_and_login
    let writer = Expert.writer
    let reader = Expert.reader
    let backend_key = Expert.backend_key
    let runtime_parameters = Expert.runtime_parameters
    let pq_cancel = pq_cancel
  end

  let server_version (t : Expert.t) =
    (* logic adapted from libpq's fe-exec.c *)
    let rec of_radix digits ~radix =
      match digits with
      | [] -> 0
      | [ x ] ->
        assert (x >= 0);
        x
      | x :: y :: digits ->
        assert (Int.between y ~low:0 ~high:(radix - 1));
        assert (Int.between x ~low:0 ~high:((Int.max_value - y) / radix));
        of_radix (((x * radix) + y) :: digits) ~radix
    in
    match
      let server_version = Hashtbl.find_exn t.runtime_parameters "server_version" in
      let new_style major minor = of_radix [ major; minor ] ~radix:(100 * 100) in
      let old_style major minor rev = of_radix [ major; minor; rev ] ~radix:100 in
      match Scanf.sscanf_opt server_version "%u.%u.%u" old_style with
      | Some version -> version
      | None ->
        (match
           Scanf.sscanf_opt server_version "%u.%u" (fun major minor ->
             if major >= 10
             then new_style major minor
             else (* old style without revision, e.g. 9.6devel *)
               old_style major minor 0)
         with
         | Some version -> version
         | None ->
           Scanf.sscanf server_version "%u" (fun major ->
             (* new style without minor version, e.g. 10devel *)
             new_style major 0))
    with
    | n -> Ok n
    | exception _ -> Error `Unknown
  ;;
end
