#' Compression zstd pour write_parquet
#' @export
write_parquet <- function(dt, path, quiet = F, ...) {
  tempname = paste0(sample(letters, 8, replace = TRUE), collapse = "")
  conn = get_conn()
  duckdb::duckdb_register(conn, tempname, dt)
  write_duckdb_parquet_raw(conn, tempname, path)
  duckdb::duckdb_unregister(conn, tempname)
}
