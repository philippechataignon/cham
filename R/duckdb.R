#' Traite un dataset arrow comme un table duckdb
#' @param ds : dataset arrow
#' @export
ds_register <- function(conn, ds)
{
    duckdb::duckdb_register_arrow(
    conn = conn,
    name = deparse(substitute(ds)),
    arrow_scannable = ds
  )
}
