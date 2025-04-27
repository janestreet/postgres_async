open! Core
open! Async
open! Import

module StartupMessage : sig
  module Parameter : sig
    module Name : sig
      (** The currently recognized parameter names. The following descriptions are copied
          from
          {{:https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-STARTUPMESSAGE}
            the PostgreSQL docs}: *)

      (** The database user name to connect as. Required; there is no default. *)
      val user : string

      (** The database to connect to. Defaults to the user name. *)
      val database : string

      (** Command-line arguments for the backend. (This is deprecated in favor of setting
          individual run-time parameters.) See the [Options] module for an implementation
          of this. *)
      val options : string

      (** Used to connect in streaming replication mode, where a small set of replication
          commands can be issued instead of SQL statements. Value can be true, false, or
          database, and the default is false. See
          {{:https://www.postgresql.org/docs/13/protocol-replication.html} Section 52.4}
          for details. *)
      val replication : string
    end

    module Options : sig
      (** Command-line arguments for the backend are delimited by spaces in the value sent
          to the server. These functions convert between a list of arguments and the
          equivalent parameter value to send to the server *)

      val encode : string list -> string
      val decode : string -> string list
    end
  end

  (** Must contain a "user" parameter. Keys may not be empty and no strings may contain a
      null byte. *)
  type t = private string String.Map.t [@@deriving compare, sexp_of]

  val find : t -> string -> string option

  (** user is required, all other parameters are optional - they can be accessed via
      [find] *)
  val user : t -> string

  (** This replicates postgres' behavior. The database is optional but if the field is
      missing then postgres will default to user. *)
  val database_defaulting_to_user : t -> string

  (** [options] defaults to the empty list *)
  val options : t -> string list

  (** These settings will be applied during backend start (after parsing the command-line
      arguments if any) and will act as session defaults. *)
  val runtime_parameters : t -> string String.Map.t

  val protocol_extensions : t -> string String.Map.t

  val create_exn
    :  user:string
    -> ?database:string
    -> ?replication:string
    -> ?options:string list
    -> ?runtime_parameters:string String.Map.t
    -> ?protocol_extensions:string String.Map.t
    -> unit
    -> t

  val of_parameters_exn : string String.Map.t -> t
  val consume : (t Or_error.t, [> read ], seek) Iobuf.Consume.t
end

module PasswordMessage : sig
  type t =
    | Cleartext_or_md5_hex of string
    | Gss_binary_blob of string

  val consume_krb : ([> read ], seek) Iobuf.t -> length:int -> t Or_error.t
  val consume_password : ([> read ], seek) Iobuf.t -> t Or_error.t
end

module Parse : sig
  type t =
    { destination : Types.Statement_name.t
    ; query : string
    }
end

module Bind : sig
  type t =
    { destination : Types.Portal_name.t
    ; statement : Types.Statement_name.t
    ; parameters : string option array
    }
end

module Execute : sig
  type num_rows =
    | Unlimited
    | Limit of int

  type t =
    { portal : Types.Portal_name.t
    ; limit : num_rows
    }
end

module Describe : sig
  type t =
    | Statement of Types.Statement_name.t
    | Portal of Types.Portal_name.t
end

module Close : sig
  type t =
    | Statement of Types.Statement_name.t
    | Portal of Types.Portal_name.t
end

module CopyFail : sig
  type t = { reason : string }
end

module Query : sig
  type t = string

  val consume : ([> read ], seek) Iobuf.t -> t Or_error.t
end

module CancelRequest : sig
  type t =
    { pid : int
    ; secret : int
    }

  val consume : ([> read ], seek) Iobuf.t -> t Or_error.t
end

module Writer : sig
  open Async

  val ssl_request : Writer.t -> unit -> unit
  val startup_message : Writer.t -> StartupMessage.t -> unit
  val password_message : Writer.t -> PasswordMessage.t -> unit
  val parse : Writer.t -> Parse.t -> unit
  val bind : Writer.t -> Bind.t -> unit
  val close : Writer.t -> Close.t -> unit
  val query : Writer.t -> Query.t -> unit
  val describe : Writer.t -> Describe.t -> unit
  val execute : Writer.t -> Execute.t -> unit
  val copy_fail : Writer.t -> CopyFail.t -> unit
  val copy_data : Writer.t -> Shared.CopyData.t -> unit
  val cancel_request : Writer.t -> CancelRequest.t -> unit
  val flush : Writer.t -> unit
  val sync : Writer.t -> unit
  val copy_done : Writer.t -> unit
  val terminate : Writer.t -> unit
end
