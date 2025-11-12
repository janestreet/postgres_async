open! Core
open Async

module type S = sig
  type t

  type 'a handle_message_result =
    | Stop of 'a
    | Continue
    | Protocol_error of Pgasync_error.t

  (** [read_messages] and will handle and dispatch the three asynchronous message types
      for you; you should never see them.

      [handle_message] is given a message type constructor, and an iobuf windowed on the
      payload of the message (that is, the message-type-specific bytes; the window does
      not include the type or message length header, they have already been consumed).

      [handle_message] must consume (as in [Iobuf.Consume]) all of the bytes of the
      message. *)
  type 'a handle_message :=
    Postgres_async_protocol.Backend.constructor
    -> (read, Iobuf.seek, Iobuf.global) Iobuf.t
    -> 'a

  type 'a read_messages_result =
    | Connection_closed of Pgasync_error.t
    | Done of 'a

  (** NoticeResponse, and ParameterStatus and NotificationResponse are 'asynchronous
      messages' and are not associated with a specific request-response conversation. They
      can happen at any time.

      [read_messages] handles these for you, and does not show them to your
      [handle_message] callback. *)
  val read_messages
    :  ?pushback:(unit -> unit Deferred.t)
    -> t
    -> handle_message:'a handle_message_result handle_message
    -> 'a read_messages_result Deferred.t

  (** See [read_messages], except [handle_message] returns the pushback instead of a
      separate function *)
  val read_messages'
    :  t
    -> handle_message:'a handle_message_result Deferred.t handle_message
    -> 'a read_messages_result Deferred.t

  (** If a message arrives while no request-response conversation (query or otherwise) is
      going on, use [consume_one_asynchronous_message] to eat it.

      If the message is one of those asynchronous messages, it will be handled. If it is
      some other message type, that is a protocol error and the connection will be closed.
      If the reader is actually at EOF, the connection will be closed with an error. *)
  val consume_one_asynchronous_message : t -> unit read_messages_result Deferred.t
end
