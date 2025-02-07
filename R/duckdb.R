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

#' Renvoit une table duckdb depuis S3 personnel
#' @param conn : connexion duckdb
#' @param path : chemin de la table/répertoire parquet
#' @export
tbl_s3 <- function(conn, path)
{
  dplyr::tbl(conn,
    glue::glue("read_parquet('s3://travail/user-{Sys.getenv(\"IDEP\")}/{path}')")
  )
}

#' Renvoit une table duckdb depuis S3 exploitations stat
#' @param conn : connexion duckdb
#' @param path : chemin de la table/répertoire parquet
#' @export
tbl_expl <- function(conn, path)
{
  dplyr::tbl(conn,
    glue::glue("read_parquet('s3://insee/sern-div-exploitations-statistiques-rp/{path}')")
  )
}
