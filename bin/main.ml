type statement_type = Statement_Insert | Statement_Select
type statement = { stmt_type : statement_type }
type parser_error = Object_Unrecognized

let do_meta_command (input : string) : (unit, parser_error) result =
  match input with
  | ".exit" ->
      print_endline "Exiting...";
      exit 0
  | _ -> Error Object_Unrecognized

let prepare_statement (input : string) : (statement, parser_error) result =
  match input with
  | "insert" -> Ok { stmt_type = Statement_Insert }
  | "select" -> Ok { stmt_type = Statement_Select }
  | _ -> Error Object_Unrecognized

let execute_statement (stmt : statement) : unit =
  match stmt.stmt_type with
  | Statement_Insert -> print_endline "This is where we would do an insert"
  | Statement_Select -> print_endline "This is where we would do an select"

module Repl = struct
  let print_prompt () =
    Printf.printf "db > ";
    flush stdout

  let rec run () =
    print_prompt ();
    let input = read_line () in

    (if input.[0] = '.' then
       match do_meta_command input with
       | Error Object_Unrecognized ->
           Printf.printf "Unrecognized Command %s\n" input
       | Ok _ -> ()
     else
       match prepare_statement input with
       | Ok stmt ->
           execute_statement stmt;
           print_endline "Executed."
       | Error Object_Unrecognized -> print_endline "Unrecognized statement");

    run ()
end

let () = Repl.run ()
