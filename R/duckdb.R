#' Traite un dataset arrow comme une table duckdb
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

#' Renvoie une connexion duckdb
#' @param dbdir : nom du la base duckdb, par défaut base stockée en mémoire
#' @export
get_conn <- function(dbdir=":memory:") {
  if (!exists("conn") || !dbIsValid(conn)) {
    conn = DBI::dbConnect(
      duckdb::duckdb(),
      dbdir = dbdir,
      bigint = "integer64"
    )
  }
  if (Sys.getenv("SITE") == "ls3") {
    DBI::dbExecute(conn, "
      LOAD httpfs;
      SET s3_url_style = 'path';
    ")
  }
  DBI::dbExecute(conn, "SET preserve_identifier_case = false")
  conn
}

