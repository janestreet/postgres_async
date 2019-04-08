open Core

type t =
  { datadir : string
  ; server_pid : Pid.t
  ; socket_dir : string
  ; port : int
  }

let postgres_bins =
  lazy (
    let candidates =
      [ "/usr/pgsql-10/bin"
      ; "/usr/pgsql-9.6/bin"
      ; "/usr/lib/postgresql/11/bin"
      ; "/usr/lib/postgresql/9.6/bin"
      ]
    in
    List.find candidates ~f:(fun dir ->
      match Sys.is_directory dir with
      | `Yes -> true
      | `No | `Unknown -> false
    )
    |> Option.value_exn ~message:"could not find a postgresql installation")

let pg_hba =
  [ "# TYPE  DATABASE        USER                 ADDRESS      METHOD"
  ; "local   all             postgres                          trust"
  ; "host    all             postgres             127.0.0.1/32 trust"
  ; "local   all             +role_password_login              md5"
  ; "host    all             +role_password_login 127.0.0.1/32 md5"
  ; ""
  ]
  |> String.concat ~sep:"\n"

let socket_dir = "/tmp"

let tempfiles_dir =
  match Sys.getenv "TMPDIR" with
  | None -> "/tmp"
  | Some d -> d

let fork_redirect_exec ~prog ~args ~stdouterr_file =
  match Unix.fork () with
  | `In_the_parent pid -> pid
  | `In_the_child ->
    Unix.dup2 ~src:stdouterr_file ~dst:Unix.stdout;
    Unix.dup2 ~src:stdouterr_file ~dst:Unix.stderr;
    never_returns (Unix.exec ~prog ~argv:(prog :: args) ())

let create ?(extra_server_args=[]) () =
  let postgres_output_filename, postgres_output =
    Unix.mkstemp (tempfiles_dir ^/ "postgres-output")
  in
  let datadir =
    Unix.mkdtemp (tempfiles_dir ^/ "postgres-datadir")
  in
  let get_postgres_output () = In_channel.read_all postgres_output_filename in
  (* Ask the OS to assign us a temporary port. *)
  let temp_socket = Unix.socket ~domain:PF_INET ~kind:SOCK_STREAM ~protocol:0 in
  Unix.bind temp_socket ~addr:(ADDR_INET (Unix.Inet_addr.of_string "0.0.0.0", 0));
  let port =
    match Unix.getsockname temp_socket with
    | ADDR_UNIX _ -> assert false
    | ADDR_INET (_, port) -> port
  in
  (* Postgres binds with SO_REUSEADDR.

     In order to allow postgres to use this port, we must set SO_REUSEADDR on
     [temp_socket] too.

     By binding the same port on [127.0.0.2] and _then_ switching REUSEADDR on
     [temp_socket2] off, it will not be possible to bind to [0.0.0.0:port], since such
     a bind will conflict with the [127.0.0.2:port] socket. In particular, another
     instance of this test will not re-use the port. But also other processes asking for
     [0.0.0.0:ephemeral] will not be given [port].

     A process asking for [127.0.0.1:ephemeral] without REUSEADDR set be handed our port.

     A process asking for an ephemeral port on [127.0.0.1] with REUSEADDR set could
     possibly be handed our port. It's very unlikely that any process actually attempts
     this. *)
  let temp_socket2 = Unix.socket ~domain:PF_INET ~kind:SOCK_STREAM ~protocol:0 in
  Unix.setsockopt temp_socket SO_REUSEADDR true;
  Unix.setsockopt temp_socket2 SO_REUSEADDR true;
  Unix.bind temp_socket2 ~addr:(ADDR_INET (Unix.Inet_addr.of_string "127.0.0.2", port));
  Unix.setsockopt temp_socket2 SO_REUSEADDR false;
  let initdb =
    let prog = force postgres_bins ^/ "initdb" in
    fork_redirect_exec
      ~prog
      ~args:[ "-D"; datadir; "-N" (* no sync *); "-U"; "postgres" ]
      ~stdouterr_file:postgres_output
  in
  match Unix.waitpid_exn initdb with
  | exception exn ->
    print_endline (get_postgres_output ());
    raise exn
  | () ->
    Out_channel.write_all (datadir ^/ "pg_hba.conf") ~data:pg_hba;
    let server_pid =
      let prog = force postgres_bins ^/ "postgres" in
      let args =
        [ "-D"; datadir
        ; "-c"; "listen_addresses=127.0.0.1"
        ; "-c"; sprintf "port=%i" port
        ; "-c"; "unix_socket_directories=" ^ socket_dir
        ; "-c"; "logging_collector=false" (* log to stdout *)
        ]
        @ extra_server_args
      in
      fork_redirect_exec
        ~prog
        ~args
        ~stdouterr_file:postgres_output
    in
    at_exit (fun () ->
      (* SIGQUIT = 'immediate shutdown' *)
      (match Signal.send Signal.quit (`Pid server_pid) with
       | `No_such_process ->
         eprintf "in at-exit handler, postgres was not alive?\n%!"
       | `Ok ->
         match Unix.waitpid_exn server_pid with
         | () -> ()
         | exception exn ->
           eprintf !"in at-exit handler, waiting for postgres failed: %{Exn}\n%!" exn);
      let pid =
        Unix.fork_exec ()
          ~prog:"rm"
          ~argv:["rm"; "-rf"; "--"; datadir; postgres_output_filename]
      in
      Unix.waitpid_exn pid
    );
    let rec wait_for_postgres ~timeout =
      match timeout < 0 with
      | true ->
        print_endline (get_postgres_output ());
        failwith "timeout waiting for postgres to start"
      | false ->
        match Unix.wait_nohang (`Pid server_pid) with
        | Some (_, exit_or_signal) ->
          print_endline (In_channel.read_all postgres_output_filename);
          raise_s [%message "postgres terminated early" (exit_or_signal : Unix.Exit_or_signal.t)]
        | None ->
          let output = get_postgres_output () in
          match String.is_substring output ~substring:"ready to accept connections" with
          | false ->
            ignore (Unix.nanosleep 0.1 : float);
            wait_for_postgres ~timeout:(timeout - 1)
          | true ->
            ()
    in
    wait_for_postgres ~timeout:100;
    { server_pid; datadir; socket_dir; port }

let create_database { socket_dir; port; _ } name =
  let pid =
    Unix.fork_exec ()
      ~prog:(force postgres_bins ^/ "psql")
      ~argv:[ "psql"
            ; "-qX"
            ; "-h"; socket_dir
            ; "-p"; Int.to_string port
            ; "-U"; "postgres"
            ; "-d"; "postgres"
            ; "--set"; "ON_ERROR_STOP"
            ; "-c"; "CREATE DATABASE " ^ name
            ]
  in
  Unix.waitpid_exn pid

let pg_hba_filename { datadir; _ } = datadir ^/ "pg_hba.conf"
let pg_ident_filename { datadir; _ } = datadir ^/ "pg_ident.conf"

let sighup_server { server_pid; _ } = Signal.send_exn Signal.hup (`Pid server_pid)

let unix_socket_path { socket_dir; port; _ } = sprintf "%s/.s.PGSQL.%i" socket_dir port
let port { port; _ } = port

open Async

let where_to_connect t =
  Tcp.Where_to_connect.of_file (unix_socket_path t)

let with_connection_exn t ?(user="postgres") ~database func =
  match%bind
    Postgres_async.with_connection
      ~user
      ~server:(where_to_connect t)
      ~database
      func
  with
  | Ok () -> return ()
  | Error err -> Error.raise err
