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
    | '\n' -> Some 'n'
    | '\r' -> Some 'r'
    | '\t' -> Some 't'
    | '\\' -> Some '\\'
    | _ -> None
  ;;

  let is_special c = Option.is_some (special_escape c)

  let row_to_string row =
    let row =
      Array.map row ~f:(fun s ->
        match s with
        | None -> None
        | Some s -> Some (s, String.count s ~f:is_special))
    in
    let total_size =
      Array.fold row ~init:0 ~f:(fun acc s ->
        match s with
        | None -> acc + 3
        | Some (s, specials) -> acc + String.length s + specials + 1)
    in
    let data = Bytes.create total_size in
    let pos =
      Array.fold row ~init:0 ~f:(fun pos s ->
        let pos =
          match s with
          | None ->
            Bytes.From_string.blit ~src:"\\N" ~src_pos:0 ~dst:data ~dst_pos:pos ~len:2;
            pos + 2
          | Some (s, 0) ->
            let len = String.length s in
            Bytes.From_string.blit ~src:s ~src_pos:0 ~dst:data ~dst_pos:pos ~len;
            pos + len
          | Some (s, _) ->
            String.fold s ~init:pos ~f:(fun pos char ->
              match special_escape char with
              | None ->
                Bytes.set data pos char;
                pos + 1
              | Some char ->
                Bytes.set data pos '\\';
                Bytes.set data (pos + 1) char;
                pos + 2)
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
