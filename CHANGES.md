## Release v0.17.0

- `Postgres_async` now returns appropriate `sql_state_code`s for connection-related error states, such as connection unexpectedly closed.

- `Postgres_async` gained implementation of the SimpleQuery message flow, which is currently exposed under `Postgres_async.Private`, as it is relatively new and not extensively tested. If you wish to try it, the main entry points are `simple_query` and `execute_simple`.

- `Postgres_async.Private.pg_cancel` is a best-effort attempt to send out-of-bound message equivalent to invocation of `pg_cancel_backend()` for the given connection.

- `Postgres_async.close` now accepts an optional `try_cancel_statement_before_close`
  parameter to try and issue out-of-band cancel request for the currently running
  statement (if any) before closing the connection.

- For the `Postgres_async.copy_in_rows` and `String_escaping.Copy_in`, the `column_names`
  parameter type changed from `string array` to `string list`.
