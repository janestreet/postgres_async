open Core
open! Int.Replace_polymorphic_compare

let escape_identifier s =
  String.split s ~on:'.'
  |> List.map ~f:(fun s ->
    "\"" ^ String.substr_replace_all s ~pattern:"\"" ~with_:"\"\"" ^ "\"")
  |> String.concat ~sep:"."
;;

(* temporary escape hatch in case we break someone's code *)
let quote_table_name_requested =
  lazy (Option.is_some (Sys.getenv "POSTGRES_ASYNC_COPY_ESCAPE_NAMES"))
;;

module Copy_in = struct
  let query ?schema_name ~table_name ~column_names () =
    let column_names =
      (if Lazy.force quote_table_name_requested
       then List.map column_names ~f:escape_identifier
       else column_names)
      |> String.concat ~sep:", "
    in
    let table_name =
      if Lazy.force quote_table_name_requested
      then escape_identifier table_name
      else table_name
    in
    let table_name =
      match schema_name with
      | None -> table_name
      | Some schema -> schema ^ "." ^ table_name
    in
    [%string
      "COPY %{table_name} ( %{column_names} ) FROM STDIN ( FORMAT text,  DELIMITER '\t')"]
  ;;

  let special_escape char =
    match char with
    | '\n' -> This 'n'
    | '\r' -> This 'r'
    | '\t' -> This 't'
    | '\\' -> This '\\'
    | _ -> Null
  ;;

  let is_special c = Or_null.is_this (special_escape c)

  let row_to_string row =
    let is_escaped = (Array.create [@alloc stack]) ~len:(Array.length row) false in
    let total_size =
      Array.foldi row ~init:0 ~f:(stack_ fun i acc s ->
        match s with
        | Null -> acc + 3
        | This s ->
          let specials = String.count s ~f:is_special in
          if specials <> 0 then Array.unsafe_set is_escaped i true;
          acc + String.length s + specials + 1)
    in
    let data = Bytes.create total_size in
    let pos =
      Array.foldi row ~init:0 ~f:(stack_ fun i pos s ->
        let pos =
          match s with
          | Null ->
            Bytes.From_string.blit ~src:"\\N" ~src_pos:0 ~dst:data ~dst_pos:pos ~len:2;
            pos + 2
          | This s ->
            (match Array.unsafe_get is_escaped i with
             | false ->
               let len = String.length s in
               Bytes.From_string.blit ~src:s ~src_pos:0 ~dst:data ~dst_pos:pos ~len;
               pos + len
             | true ->
               String.fold s ~init:pos ~f:(stack_ fun pos char ->
                 match special_escape char with
                 | Null ->
                   Bytes.set data pos char;
                   pos + 1
                 | This char ->
                   Bytes.set data pos '\\';
                   Bytes.set data (pos + 1) char;
                   pos + 2))
        in
        Bytes.set data pos '\t';
        pos + 1)
    in
    assert (pos = Bytes.length data);
    Bytes.set data (pos - 1) '\n';
    Bytes.unsafe_to_string ~no_mutation_while_string_reachable:data
  ;;
end

module Listen = struct
  let query ~channel = sprintf !"LISTEN %{escape_identifier}" channel
end
