Postgres_async
--------------
`postgres_async` is an ocaml PostgreSQL client that implements the PostgreSQL protocol
rather than binding to the libpq C library. It provides support for regular queries
(including support for 'parameters': `SELECT * WHERE a = $1`) and `COPY IN` mode. The
interface presented is minimal to keep the library simple for now, though in the future
a layer on top may add convenience functions."

To get started, have a look at the Postgres_async module interface in
`src/postgresql_async.mli`.
