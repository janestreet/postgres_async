open Core

module Frontend : sig
  module StartupMessage : sig
    type t =
      { user : string
      ; database : string
      }
  end

  module PasswordMessage : sig
    type t =
      | Cleartext_or_md5_hex of string
      | Gss_binary_blob of string
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
      | Portal    of Types.Portal_name.t
  end

  module Close : sig
    type t =
      | Statement of Types.Statement_name.t
      | Portal    of Types.Portal_name.t
  end

  module CopyFail : sig
    type t =
      { reason : string
      }
  end

  module CopyData : sig
    type t = string
  end

  module Writer : sig
    open Async

    val startup_message  : Writer.t -> StartupMessage.t  -> unit
    val password_message : Writer.t -> PasswordMessage.t -> unit
    val parse            : Writer.t -> Parse.t           -> unit
    val bind             : Writer.t -> Bind.t            -> unit
    val close            : Writer.t -> Close.t           -> unit
    val describe         : Writer.t -> Describe.t        -> unit
    val execute          : Writer.t -> Execute.t         -> unit
    val copy_fail        : Writer.t -> CopyFail.t        -> unit
    val copy_data        : Writer.t -> CopyData.t        -> unit

    val flush     : Writer.t -> unit
    val sync      : Writer.t -> unit
    val copy_done : Writer.t -> unit
    val terminate : Writer.t -> unit
  end
end

module Backend : sig
  type constructor =
    | AuthenticationRequest
    | BackendKeyData
    | BindComplete
    | CloseComplete
    | CommandComplete
    | CopyData
    | CopyDone
    | CopyInResponse
    | CopyOutResponse
    | CopyBothResponse
    | DataRow
    | EmptyQueryResponse
    | ErrorResponse
    | FunctionCallResponse
    | NoData
    | NoticeResponse
    | NotificationResponse
    | ParameterDescription
    | ParameterStatus
    | ParseComplete
    | PortalSuspended
    | ReadyForQuery
    | RowDescription
  [@@deriving sexp, compare]

  val focus_on_message
    :  ([> read], Iobuf.seek) Iobuf.t
    -> (constructor, [ `Unknown_message_type of char | `Too_short ]) Result.t

  module ErrorResponse : sig
    type t = Error.t

    val consume : ([> read], Iobuf.seek) Iobuf.t -> t Or_error.t
  end

  module NoticeResponse : sig
    type t = Info.t

    val consume : ([> read], Iobuf.seek) Iobuf.t -> t Or_error.t
  end

  module AuthenticationRequest : sig
    (** [Ok] is not actually a request. It means that auth has succeeded. *)
    type t =
      | Ok
      | KerberosV5
      | CleartextPassword
      | MD5Password of { salt : string }
      | SCMCredential
      | GSS
      | SSPI
      | GSSContinue of { data : string }
    [@@deriving sexp]

    val consume : ([> read], Iobuf.seek) Iobuf.t -> t Or_error.t
  end

  module ParameterStatus : sig
    type t =
      { key : string
      ; data : string
      }

    val consume : ([> read], Iobuf.seek) Iobuf.t -> t Or_error.t
  end

  module BackendKeyData : sig
    type t = Types.backend_key

    val consume : ([> read], Iobuf.seek) Iobuf.t -> t Or_error.t
  end

  module NotificationResponse : sig
    type t =
      { pid : Pid.t
      ; channel : Types.Notification_channel.t
      ; payload : string
      }

    val consume : ([> read], Iobuf.seek) Iobuf.t -> t Or_error.t
  end

  module ReadyForQuery : sig
    type t =
      | Idle
      | In_transaction
      | In_failed_transaction
    [@@deriving sexp_of]

    val consume : ([> read], Iobuf.seek) Iobuf.t -> t Or_error.t
  end

  module ParseComplete : sig
    val consume : ([> read], Iobuf.seek) Iobuf.t -> unit
  end

  module BindComplete : sig
    val consume : ([> read], Iobuf.seek) Iobuf.t -> unit
  end

  module NoData : sig
    val consume : ([> read], Iobuf.seek) Iobuf.t -> unit
  end

  module EmptyQueryResponse : sig
    val consume : ([> read], Iobuf.seek) Iobuf.t -> unit
  end

  module CopyDone : sig
    val consume : ([> read], Iobuf.seek) Iobuf.t -> unit
  end

  module RowDescription : sig
    (** Technically [format] could be [`Binary], but since [Frontend.Bind]
        doesn't ever ask for binary output right now, it's impossible to receive
        it from the server, and [consume] will reject it for simplicity. *)
    type column =
      { name : string
      ; format : [ `Text ]
      }

    type t = column array

    val consume : ([> read], Iobuf.seek) Iobuf.t -> t Or_error.t
  end

  module DataRow : sig
    type t = string option array

    val consume : ([> read], Iobuf.seek) Iobuf.t -> t Or_error.t

    val skip :  ([> read], Iobuf.seek) Iobuf.t -> unit
  end

  module type CopyResponse = sig
    (** Unlike in [RowDescription], it is possible to receive [`Binary] here
        because someone could put that option in their COPY query.
        [Postgres_async] will then abort the copy. *)
    type column =
      { name : string
      ; format : [ `Text | `Binary ]
      }

    type t =
      { overall_format : [ `Text | `Binary ]
      ; num_columns : int
      ; column_formats : [ `Text | `Binary ] array
      }

    val consume : ([> read], Iobuf.seek) Iobuf.t -> t Or_error.t
  end

  module CopyInResponse : CopyResponse
  module CopyOutResponse : CopyResponse

  module CopyData : sig
    val skip : ([> read], Iobuf.seek) Iobuf.t -> unit
  end

  module CommandComplete : sig
    type t = string

    val consume : ([> read], Iobuf.seek) Iobuf.t -> t Or_error.t
  end
end
