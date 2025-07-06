type row = { id : int; username : string; email : string }

module Table = struct
  let id_size = 4
  let username_size = 32
  let email_size = 255
  let id_offset = 0
  let username_offset = id_offset + id_size
  let email_offset = username_offset + username_size
  let row_size = id_size + username_size + email_size
  let page_size = 4096
  let table_max_pages = 100
  let rows_per_page = page_size / row_size
  let _table_max_rows = rows_per_page * table_max_pages

  type t = { mutable num_rows : int; pages : bytes option array }

  let create () = { num_rows = 0; pages = Array.make table_max_pages None }

  let serialize_row (source : row) (page : bytes) (offset : int) : unit =
    Bytes.set_int32_le page (offset + id_offset) (Int32.of_int source.id);

    let user_bytes = Bytes.create username_size in
    let email_bytes = Bytes.create email_size in
    Bytes.blit_string source.username 0 user_bytes 0
      (min (String.length source.username) username_size);
    Bytes.blit user_bytes 0 page (offset + username_offset) username_size;

    Bytes.blit_string source.email 0 email_bytes 0
      (min (String.length source.email) email_size);
    Bytes.blit email_bytes 0 page (offset + email_offset) email_size

  let deserialize_row (page : bytes) (offset : int) : row =
    let id = Bytes.get_int32_le page (offset + id_offset) |> Int32.to_int in

    let username_bytes =
      Bytes.sub page (offset + username_offset) username_size
    in
    let username =
      Bytes.unsafe_to_string
        (Bytes.split_on_char '\000' username_bytes |> List.hd)
    in

    let email_bytes = Bytes.sub page (offset + email_offset) email_size in
    let email =
      Bytes.unsafe_to_string (Bytes.split_on_char '\000' email_bytes |> List.hd)
    in

    { id; username; email }

  let row_slot (table : t) (row_num: int) : bytes * int =
    let page_num = row_num / rows_per_page in

    let page =
      match table.pages.(page_num) with
      | Some page -> page
      | None ->
          let new_page = Bytes.create page_size in
          table.pages.(page_num) <- Some new_page;
          new_page
    in

    let row_offset = row_num mod rows_per_page in
    let byte_offset = row_offset * row_size in
    (page, byte_offset)
end

module Repl = struct
  type statement_type = Statement_Insert | Statement_Select
  type statement = { stmt_type : statement_type; content : row option }
  type parser_error = Object_Unrecognized
  type execute_error = Execute_Table_Full

  let print_prompt () =
    Printf.printf "db > ";
    flush stdout

  let do_meta_command (input : string) : (unit, parser_error) result =
    match input with
    | ".exit" ->
        print_endline "Exiting...";
        exit 0
    | _ -> Error Object_Unrecognized

  let parse_insert (input : string) : (row, parser_error) result =
    try
      Scanf.sscanf input "insert %d %s %s" (fun id username email ->
          Ok { id; username; email })
    with Scanf.Scan_failure _ -> Error Object_Unrecognized

  let prepare_statement (input : string) : (statement, parser_error) result =
    if String.starts_with ~prefix:"insert" input then
      Result.map
        (fun row -> { stmt_type = Statement_Insert; content = Some row })
        (parse_insert input)
    else if String.starts_with ~prefix:"select" input then
      Ok { stmt_type = Statement_Select; content = None }
    else Error Object_Unrecognized

  let execute_insert (stmt : statement) (table : Table.t) :
      (unit, execute_error) result =
    if table.num_rows >= Table.table_max_pages then Error Execute_Table_Full
    else
      let row_to_insert = stmt.content |> Option.get in
      let page, offset = Table.row_slot table table.num_rows in
      Table.serialize_row row_to_insert page offset;
      table.num_rows <- table.num_rows + 1;
      Ok ()

  let execute_select (_statement : statement) (table : Table.t) :
      (unit, execute_error) result =
    for i = 0 to table.num_rows - 1 do
      let pages, offset = Table.row_slot table i in
      let row = Table.deserialize_row pages offset in
      Printf.printf "(%d, %s, %s)\n" row.id row.username row.email
    done;
    Ok ()

  let execute_statement (stmt : statement) (table : Table.t) :
      (unit, execute_error) result =
    match stmt.stmt_type with
    | Statement_Insert -> execute_insert stmt table
    | Statement_Select -> execute_select stmt table

  let rec run (table : Table.t) : unit =
    print_prompt ();
    let input = read_line () in

    (if input.[0] = '.' then
       match do_meta_command input with
       | Error Object_Unrecognized ->
           Printf.printf "Unrecognized Command %s\n" input
       | Ok _ -> run table
     else
       match prepare_statement input with
       | Ok stmt -> (
           match execute_statement stmt table with
          | Ok _ -> print_endline "Executed."; run table
           | Error Execute_Table_Full ->
            print_endline "Execute Error... Table is full")
        | Error Object_Unrecognized -> print_endline "Unrecognized statement");

end

let () =
  let table = Table.create () in
  Repl.run table
