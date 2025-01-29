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
#' @return Liste de 5 éléments nommés 'pind', 'plog', 'cind', 'clog', 'cfam'
#' selon principal (p)/complémentaire(c) et individu(ind)/logement(log)/famille(fam)
#' @examples
#' conn <- get_conn()
#' trp21 <- get_rp21(conn)
#' # population France
#' trp21$pind |>
#'   dplyr::count(wt = ipondi)
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

#' Renvoie une liste des noms de fichiers parquet du coffre RP EDL
#' @param an : année du RP, exemple 2015
#' @return Liste de 5 éléments nommés 'pind', 'plog', 'cind', 'clog', 'cfam'
#' selon principal (p)/complémentaire(c) et individu(ind)/logement(log)/famille(fam)
#' @examples
#' conn <- get_conn()
#' trp2015 <- get_tbl(conn, rp_edl_file(2015))
#' @export
rp_edl_files <- function(an)
{
  an2 = an %% 100
  angeo2 = (an + 2) %% 100
  list(
    "pind" = glue::glue("X:/HAB-Pole-EDL-BasesRP/RP{an2}/PARQUET/prin_ind{angeo2}"),
    "plog" = glue::glue("X:/HAB-Pole-EDL-BasesRP/RP{an2}/PARQUET/prin_log{angeo2}"),
    "cind" = glue::glue("X:/HAB-Pole-EDL-BasesRP/RP{an2}/PARQUET/compl_ind{angeo2}"),
    "clog" = glue::glue("X:/HAB-Pole-EDL-BasesRP/RP{an2}/PARQUET/compl_log{angeo2}"),
    "cfam" = glue::glue("X:/HAB-Pole-EDL-BasesRP/RP{an2}/PARQUET/compl_fam{angeo2}")
  )
}

#' Renvoie une liste des tables duckdb du coffre RP EDL
#' @param conn : connexion duckdb, peut être obtenu par la fonction get_conn
#' @param files : liste des fichiers obtenu par la fonction rp_edl_files(an) par
#' exemple
#' @return Liste de 5 éléments nommés 'pind', 'plog', 'cind', 'clog', 'cfam'
#' selon principal (p)/complémentaire(c) et individu(ind)/logement(log)/famille(fam)
#' @examples
#' conn <- get_conn()
#' trp2015 <- get_tbl(conn, rp_edl_file(2015))
#' @export
get_tbl <- function(conn, files)
{
  lapply(
    files,
    function(file) {
      dplyr::tbl(conn, glue::glue("read_parquet('{file}/*.parquet')"))
    }
  )
}

#' Renvoie une liste de datasets RP EDL
#' @param files : liste des fichiers obtenu par la fonction rp_edl_files(an) par
#' exemple
#' @return Liste de 5 éléments nommés 'pind', 'plog', 'cind', 'clog', 'cfam'
#' selon principal (p)/complémentaire(c) et individu(ind)/logement(log)/famille(fam)
#' @examples
#' ds_rp2015 <- get_ds(rp_edl_file(2015))
#' @export
get_ds <- function(files)
{
  lapply(
    files,
    function(file) {
      arrow::open_dataset(file)
    }
  )
}
