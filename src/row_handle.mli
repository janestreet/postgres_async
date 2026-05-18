open! Core
module Column_metadata := Postgres_async_protocol.Column_metadata

type value := (read, Iobuf.no_seek, Iobuf.global) Iobuf.t or_null

(** The safest interfaces to this module are [iteri] and [foldi]. They should generally be
    preferred unless you have some compiler-aided check to ensure you are accessing
    columns in the correct order.

    If you are generating code, or otherwise doing something where you define both the
    query and handler together so that you can statically guarantee the number and order
    of result columns without even inspecting the [columns]: Use [unchecked_next] for best
    performance.

    If you can't use [foldi] or [iteri], but you don't have a nice static safety
    guarantee, there is [next], which at least ensures you don't attempt to read past the
    end of the row. This should probably only be used when you're hand-writing both the
    query and the row handler.

    Important note for users of [next] and [unchecked_next]:

    This row handle contains a reference to the raw internal reader buffer of the socket
    reader, so references to underlying buffers should not be held once functions return.

    This also means that consumers must consume *every* column of the row, otherwise
    [Postgres_async] will raise a protcol error. *)

type t : value mod non_float portable

val columns : t @ local -> Column_metadata.t iarray

(** Consume the next column of the row. If there are no remaining columns, return [None].

    If you need to seek in [value], use [Iobuf.sub_shared__local]. *)
val next : t @ local -> f:(value @ local -> 'a) @ local once -> 'a or_null

(** Like [next], but without the check that there are columns remaining in the row, nor a
    check to prevent you from calling [foldi]/[iteri].

    If you can guarantee that you call [unchecked_next] exactly once per column, this is
    safe. *)
val unchecked_next
  : ('a : value_or_null).
  t @ local -> f:(value @ local -> 'a) @ local once -> 'a

(** [foldi] is a convenience alias for [unchecked_next] in [Array.iter (columns t)].
    Calling [foldi] after any any other method (besides [columns]) is an error and will
    raise. *)
val foldi
  : ('acc : value_or_null).
  t @ local
  -> init:'acc
  -> f:(column:Column_metadata.t -> value:value @ local -> 'acc -> 'acc) @ local
  -> 'acc

(** see [foldi] *)
val iteri
  :  t @ local
  -> f:(column:Column_metadata.t -> value:value @ local -> unit) @ local
  -> unit

(**/**)

module Private : sig
  val create
    :  Column_metadata.t iarray
    -> datarow:([> read ], Iobuf.seek, Iobuf.global) Iobuf.t @ local
    -> t @ local
end
