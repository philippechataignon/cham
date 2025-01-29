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
  conn
}

get_rp21_root <- function(conn, gen_root)
{
  list(
    pind = dplyr::tbl(conn, glue::glue("read_parquet([
        '{gen_root}/A100021/GEN_A1000214_DIMETAPARQUET/META.parquet',
        '{gen_root}/A100021/GEN_A1000214_DIMETBPARQUET/METB.parquet',
        '{gen_root}/A100021/GEN_A1000214_DIMETCPARQUET/METC.parquet',
        '{gen_root}/A100021/GEN_A1000215_DIDOMPARQUET/DOM.parquet'
        ])"
    )) |>
      dplyr::rename_with(tolower),
    plog = dplyr::tbl(conn, glue::glue("read_parquet([
        '{gen_root}/A100021/GEN_A1000214_DMMETPARQUET/MET.parquet',
        '{gen_root}/A100021/GEN_A1000215_DMDOMPARQUET/DOM.parquet'
        ])"
    )) |>
      dplyr::rename_with(tolower),
    cind = dplyr::tbl(conn, glue::glue("read_parquet([
        '{gen_root}/A100021/GEN_A1000212_DIMETPARQUET/MET.parquet',
        '{gen_root}/A100021/GEN_A1000216_DIDOMPARQUET/DOM.parquet'
        ])"
    )) |>
      dplyr::rename_with(tolower),
    clog = dplyr::tbl(conn, glue::glue("read_parquet([
        '{gen_root}/A100021/GEN_A1000212_DMMETPARQUET/MET.parquet',
        '{gen_root}/A100021/GEN_A1000216_DMDOMPARQUET/DOM.parquet'
        ])"
    )) |>
      dplyr::rename_with(tolower),
    cfam = dplyr::tbl(conn, glue::glue("read_parquet([
        '{gen_root}/A100021/GEN_A1000212_DFMETPARQUET/MET.parquet',
        '{gen_root}/A100021/GEN_A1000216_DFDOMPARQUET/DOM.parquet'
        ])"
    )) |>
      dplyr::rename_with(tolower)
  )
}

#' Renvoie une liste des tables du RP21
#' @param conn : connexion duckdb, peut être obtenu par la fonction get_conn
#' @note La variable d'environnement SITE doit être défini à 'ls3' ou 'aus'.
#' Par défaut, 'aus'.
#' @export
get_rp21 <- function(conn)
{
  if (Sys.getenv("SITE") == "ls3") {
    DBI::dbExecute(conn, "
      LOAD httpfs;
      SET s3_url_style = 'path';
    ")
    get_rp21_root(conn, "s3://mad/insee")
  } else {
    get_rp21_root(conn, "W:/")
  }
}
