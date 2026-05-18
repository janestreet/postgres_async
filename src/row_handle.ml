open! Core
module Column_metadata = Postgres_async_protocol.Column_metadata

type seek = Iobuf.seek
type no_seek = Iobuf.no_seek

module Function = struct
  type t =
    | Create
    | Next
    | Foldi_or_iteri
end

type t =
  { global_ columns : Column_metadata.t iarray
  ; datarow : (read, seek, Iobuf.global) Iobuf.t
  (** This is internal to the socket's reader *)
  ; mutable last_function : Function.t
  }

module Private = struct
  let create columns ~(datarow @ local) = exclave_
    let num_fields = Iobuf.Consume.uint16_be datarow in
    if num_fields <> Iarray.length columns
    then
      raise_s
        [%message
          "number of columns in DataRow message did not match RowDescription"
            ~row_description:(columns : Column_metadata.t iarray)
            (num_fields : int)]
    else { columns; datarow = Iobuf.read_only__local datarow; last_function = Create }
  ;;
end

let columns (t @ local) = t.columns

let unchecked_next
  ({ datarow; _ } @ local)
  ~(f : ((read, no_seek, Iobuf.global) Iobuf.t or_null @ local -> _) @ local)
  =
  let len = Iobuf.Consume.int32_be datarow in
  if len = -1
  then f Null
  else (
    let hi_bound = Iobuf.Hi_bound.window datarow in
    (* Narrow the window to just this one column. *)
    Iobuf.resize datarow ~len;
    let result = f (This (Iobuf.no_seek__local datarow)) in
    Iobuf.bounded_flip_hi datarow hi_bound;
    (* Set the window to begin at the next column's length and end at the end of the row. *)
    result)
;;

let next
  (t @ local)
  ~(f : ((read, no_seek, Iobuf.global) Iobuf.t or_null @ local -> _) @ local)
  =
  let () =
    match t.last_function with
    | Create -> t.last_function <- Next
    | Next -> ()
    | Foldi_or_iteri ->
      raise_s
        [%sexp
          "cannot call [Row_handle.next] after calling \
           [Row_handle.foldi]/[Row_handle.iteri]"]
  in
  if Iobuf.is_empty t.datarow
  then Null
  else
    This
      (let len = Iobuf.Consume.int32_be t.datarow in
       if len = -1
       then f Null
       else (
         let hi_bound = Iobuf.Hi_bound.window t.datarow in
         (* Narrow the window to just this one column. *)
         Iobuf.resize t.datarow ~len;
         Exn.protect
           ~f:(stack_ fun () -> f (This (Iobuf.no_seek__local t.datarow)) [@nontail])
           ~finally:(stack_ fun () -> Iobuf.bounded_flip_hi t.datarow hi_bound)))
;;

let foldi (t @ local) ~init ~(f @ local) =
  match t.last_function with
  | Next | Foldi_or_iteri ->
    raise_s
      [%sexp
        "cannot call [Row_handle.foldi]/[Row_handle.iteri] once any column has been \
         consumed"]
  | Create ->
    t.last_function <- Foldi_or_iteri;
    Iarray.fold t.columns ~init ~f:(stack_ fun acc column ->
      unchecked_next t ~f:(stack_ fun value -> f ~column ~value acc) [@nontail])
    [@nontail]
;;

let iteri (t @ local) ~(f @ local) =
  foldi t ~init:() ~f:(stack_ fun ~column ~value () -> f ~column ~value [@nontail])
  [@nontail]
;;
