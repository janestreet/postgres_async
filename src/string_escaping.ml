open Core
open! Int.Replace_polymorphic_compare

let escape_identifier s =
  "\"" ^ String.substr_replace_all s ~pattern:"\"" ~with_:"\"\"" ^ "\""

module Copy_in = struct
  let query ~table_name ~column_names =
    let column_names =
      Array.map column_names ~f:escape_identifier
      |> Array.to_list
      |> String.concat ~sep:", "
    in
    sprintf
      !"COPY %{escape_identifier} ( %s ) FROM STDIN ( FORMAT text,  DELIMITER '\t' )"
      table_name
      column_names

  let special_escape char =
    match char with
    | '\n' -> Some 'n'
    | '\r' -> Some 'r'
    | '\t' -> Some 't'
    | '\\' -> Some '\\'
    | _ -> None

  let is_special c = Option.is_some (special_escape c)

  let row_to_string row =
    let row =
      Array.map row ~f:(fun s ->
        match s with
        | None -> None
        | Some s -> Some (s, String.count s ~f:is_special)
      )
    in
    let total_size =
      Array.fold row ~init:0 ~f:(fun acc s ->
        match s with
        | None -> acc + 3
        | Some (s, specials) -> acc + String.length s + specials + 1
      )
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
                pos + 2
            )
        in
        Bytes.set data pos '\t';
        pos + 1
      )
    in
    assert (pos = Bytes.length data);
    Bytes.set data (pos - 1) '\n';
    Bytes.unsafe_to_string ~no_mutation_while_string_reachable:data
end

module Listen = struct
  let query ~channel =
    sprintf !"LISTEN %{escape_identifier}" channel
end
