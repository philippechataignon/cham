#' Renvoie une connexion duckdb
#' @param dbdir : nom du la base duckdb, par défaut base stockée en mémoire
#' @export
get_conn <- function(dbdir=":memory:") {
  if (!exists("conn") || !DBI::dbIsValid(conn)) {
    conn = DBI::dbConnect(
      duckdb::duckdb(),
      dbdir = dbdir,
      bigint = "integer64"
    )
  }
  if (Sys.getenv("SITE") %in% c("ls3", "ssp")) {
    DBI::dbExecute(conn, "
      LOAD httpfs;
      SET s3_url_style = 'path';
    ")
  }
  conn
}
#' Renvoit une table duckdb depuis un fichier parquet y.c S3
#' @param conn : connexion duckdb
#' @param path : chemin de la table/répertoire parquet
#' @param tolower : si TRUE, le défaut, les variables sont converties
#' en minuscules
#' @export
tbl_pqt <- function(conn, path, lowercase = TRUE) {
  table <- dplyr::tbl(conn, paste0("read_parquet('", path, "')"))
  if (lowercase) {
    table <- rename_with(table, tolower)
  }
  table
}

#' Renvoit une liste de tables duckdb depuis une liste
#' de chemin vers des fichiers/répertoires parquet
#' @param conn : connexion duckdb
#' @param liste : liste de chemins
#' @export
tbl_list <- function(conn, paths) {
  lapply(
    paths,
    function(x) {
      tbl_pqt(conn, x)
    }
  )
}

#' @export
s3perso <- paste0("s3://travail/user-", Sys.getenv("IDEP"))

#' @export
s3expl <- "s3://insee/sern-div-exploitations-statistiques-rp"

#' Renvoit une table duckdb depuis S3 personnel
#' @param conn : connexion duckdb
#' @param path : chemin de la table/répertoire parquet
#' @export
tbl_s3 <- function(conn, path)
{
  tbl_pqt(conn, file.path(s3perso, path))
}

#' Renvoit une table duckdb depuis S3 exploitations stat
#' @param conn : connexion duckdb
#' @param path : chemin de la table/répertoire parquet
#' @export
tbl_expl <- function(conn, path)
{
  tbl_pqt(conn, file.path(s3expl, path))
}


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

