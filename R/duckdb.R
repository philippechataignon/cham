#' Renvoie une connexion duckdb
#' @param dbdir : nom du la base duckdb, par défaut base stockée en mémoire
#' @export
get_conn <- function(dbdir = ":memory:") {
  if (!exists("conn") || !DBI::dbIsValid(conn)) {
    conn = DBI::dbConnect(
      duckdb::duckdb(),
      dbdir = dbdir,
      bigint = "integer64"
    )
  }
  if (Sys.getenv("SITE") %in% c("ls3", "ssp")) {
    DBI::dbExecute(
      conn,
      "
      INSTALL httpfs;
      LOAD httpfs;
      SET s3_url_style = 'path';
      "
    )
  }
  conn
}
#' Renvoit une table duckdb depuis un fichier parquet y.c S3
#' @param conn : connexion duckdb
#' @param path : chemin de la table/répertoire parquet
#' @param level : nombre de niveaux dans le cas de fichiers parquet partitionnés,
#' par défaut 0 si pas de partionnement
#' @param lower : si TRUE, les variables sont converties en minuscules
#' @return Liste de tables duckdb
#' @export
tbl_pqt <- function(conn, path, level = 0, lower = FALSE) {
  # si path se termine par un /, alors on ajoute *.parquet
  if (level == 0 && substr(path, nchar(path), nchar(path)) == "/") {
    path = paste0(path, "*.parquet")
  }
  if (level == 0) {
    table <- dplyr::tbl(conn, paste0("read_parquet('", path, "')"))
  } else {
    niv <- paste0(rep('*/', level), collapse = "")
    table <- dplyr::tbl(conn, paste0("read_parquet('", path, "/", niv, "/*.parquet')"))
  }
  if (lower) {
    table <- rename_with(table, tolower)
  }
  table
}

#' Renvoit une liste de tables duckdb depuis une liste
#' de chemins vers des fichiers/répertoires parquet
#' @param conn : connexion duckdb
#' @param liste : liste de chemins
#' @export
tbl_list <- function(conn, paths, level = 0, lower = FALSE) {
  lapply(
    paths,
    function(x) {
      tbl_pqt(conn, x, level, lower)
    }
  )
}

site = get_site()
if (site == "ls3") {
  s3perso <- paste0("s3://travail/user-", Sys.getenv("IDEP"))
} else {
  s3perso <- "~/work/data"
}

if (site == "ls3") {
  s3expl <- "s3://insee/sern-div-exploitations-statistiques-rp"
} else {
  s3expl <- "~/work/insee"
}

#' Bucket perso
#' @name s3perso
#' @export
s3perso

#' Bucket exploitations statistiques
#' @name s3expl
#' @export
s3expl
