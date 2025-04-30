get_rp20_root <- function(conn, gen_root) {
  list(
    pind = dplyr::tbl(
      conn,
      glue::glue(
        "read_parquet([
        '{gen_root}/A100020/GEN_A1000204_DIMETPARQUET/MET.parquet',
        '{gen_root}/A100020/GEN_A1000205_DIDOMPARQUET/DOM.parquet'
        ])"
      )
    ) |>
      dplyr::rename_with(tolower),
    plog = dplyr::tbl(
      conn,
      glue::glue(
        "read_parquet([
        '{gen_root}/A100020/GEN_A1000204_DMMETPARQUET/MET.parquet',
        '{gen_root}/A100020/GEN_A1000205_DMDOMPARQUET/DOM.parquet'
        ])"
      )
    ) |>
      dplyr::rename_with(tolower),
    cind = dplyr::tbl(
      conn,
      glue::glue(
        "read_parquet([
        '{gen_root}/A100020/GEN_A1000202_DIMETPARQUET/MET.parquet',
        '{gen_root}/A100020/GEN_A1000206_DIDOMPARQUET/DOM.parquet'
        ])"
      )
    ) |>
      dplyr::rename_with(tolower),
    clog = dplyr::tbl(
      conn,
      glue::glue(
        "read_parquet([
        '{gen_root}/A100020/GEN_A1000202_DMMETPARQUET/MET.parquet',
        '{gen_root}/A100020/GEN_A1000206_DMDOMPARQUET/DOM.parquet'
        ])"
      )
    ) |>
      dplyr::rename_with(tolower),
    cfam = dplyr::tbl(
      conn,
      glue::glue(
        "read_parquet([
        '{gen_root}/A100020/GEN_A1000202_DFMETPARQUET/MET.parquet',
        '{gen_root}/A100020/GEN_A1000206_DFDOMPARQUET/DOM.parquet'
        ])"
      )
    ) |>
      dplyr::rename_with(tolower)
  )
}

get_rp21_root <- function(conn, gen_root) {
  list(
    pind = dplyr::tbl(
      conn,
      glue::glue(
        "read_parquet([
        '{gen_root}/A100021/GEN_A1000214_DIMETAPARQUET/META.parquet',
        '{gen_root}/A100021/GEN_A1000214_DIMETBPARQUET/METB.parquet',
        '{gen_root}/A100021/GEN_A1000214_DIMETCPARQUET/METC.parquet',
        '{gen_root}/A100021/GEN_A1000215_DIDOMPARQUET/DOM.parquet'
        ])"
      )
    ) |>
      dplyr::rename_with(tolower),
    plog = dplyr::tbl(
      conn,
      glue::glue(
        "read_parquet([
        '{gen_root}/A100021/GEN_A1000214_DMMETPARQUET/MET.parquet',
        '{gen_root}/A100021/GEN_A1000215_DMDOMPARQUET/DOM.parquet'
        ])"
      )
    ) |>
      dplyr::rename_with(tolower),
    cind = dplyr::tbl(
      conn,
      glue::glue(
        "read_parquet([
        '{gen_root}/A100021/GEN_A1000212_DIMETPARQUET/MET.parquet',
        '{gen_root}/A100021/GEN_A1000216_DIDOMPARQUET/DOM.parquet'
        ])"
      )
    ) |>
      dplyr::rename_with(tolower),
    clog = dplyr::tbl(
      conn,
      glue::glue(
        "read_parquet([
        '{gen_root}/A100021/GEN_A1000212_DMMETPARQUET/MET.parquet',
        '{gen_root}/A100021/GEN_A1000216_DMDOMPARQUET/DOM.parquet'
        ])"
      )
    ) |>
      dplyr::rename_with(tolower),
    cfam = dplyr::tbl(
      conn,
      glue::glue(
        "read_parquet([
        '{gen_root}/A100021/GEN_A1000212_DFMETPARQUET/MET.parquet',
        '{gen_root}/A100021/GEN_A1000216_DFDOMPARQUET/DOM.parquet'
        ])"
      )
    ) |>
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
get_rp <- function(conn, an = 2021, src = c("gen", "edl")) {
  src = match.arg(src)
  site = get_site()
  if (src == "gen") {
    if (!an %in% 2020:2021) stop("'an' doit valoir 2020 ou 2021 pour la source 'gen'")
    if (site %in% c("ls3", "aus")) {
      if (an == 2021) {
        get_func <- get_rp21_root}
      else if (an == 2020) {
        get_func <- get_rp20_root
      }
    }
    if (site == "ls3") {
      ret = get_func(conn, "s3://mad/insee")
    } else if (Sys.getenv("SITE") == "aus") {
      ret = get_func(conn, "W:/")
    } else {
      pat = gsub('{an}', an, "~/work/insee/rp/an={an}/{x}/*", fixed = T)
      ret = tbl_list(conn, extend(rp_ext, pat))
    }
  } else if (src == "edl") {
    an2 = an %% 100
    angeo2 = (an + 2) %% 100
    cvt = list(
      "pind" = "prin_ind",
      "plog" = "prin_log",
      "cind" = "compl_ind",
      "clog" = "compl_log",
      "cfam" = "compl_fam"
    )
    if (site == "aus"){
      root = "X:/HAB-Pole-EDL-BasesRP"
    }
    paths = extend(
      cvt[rp_ext],
      paste0(root, "/RP", an2, "/PARQUET/{x}", angeo2, "/")
    )
    ret = tbl_list(conn, paths)
    names(ret) = rp_ext
  }
  ret
}

#' Extensions RP
#' @export
rp_ext = c("pind", "plog", "cind", "clog", "cfam")

#' Extensions EAR
#' @export
ear_ext = c("ind", "log", "fam", "liens")

#' Renvoie une liste des fichiers EAR
#' @param conn : connexion duckdb, peut être obtenu par la fonction get_conn
#' @note La variable d'environnement SITE doit être défini à 'ls3' ou 'aus'.
#' Par défaut, 'aus'.
#' @return Liste de 4 éléments nommés 'ind', 'log', 'fam', 'liens' pour être passée
#' à `get_tbl` ou `get_ds` selon exemple ci-dessous
#' @examples
#' conn <- get_conn()
#' tear <- tbl_pqt(conn, ear_files(), level=2)
#' tear$ind |>
#'   dplyr::group_by(an) |>
#'   dplyr::count() |>
#'   dplyr::arrange(an) |>
#'   print()
#'
#' s3fs <- s3createfs()
#' dsear = lapply(ear_files(), function(x) arrow::open_dataset(s3fs$path(substr(x, 6, nchar(x)))))
#' dsear$ind |>
#'   dplyr::group_by(an) |>
#'   dplyr::count() |>
#'   dplyr::arrange(an) |>
#'   dplyr::collect() |>
#'   print()
#' @export
ear_files <- function() {
  site = get_site()
  if (site == "ls3") {
    extend(ear_ext, "s3://insee/sern-div-exploitations-statistiques-rp/ear/{x}")
  } else if (site == "aus") {
    extend(ear_ext, "X:/HAB-MaD-SeRN/ear/{x}")
  } else {
    extend(ear_ext, "~/work/insee/ear/{x}")
  }
}
