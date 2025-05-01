rp_gen_files = list(
  "2020" = list(
    pind = c(
        '{x}/A100020/GEN_A1000204_DIMETPARQUET/MET.parquet',
        '{x}/A100020/GEN_A1000205_DIDOMPARQUET/DOM.parquet'
        ),
    plog = c(
        '{x}/A100020/GEN_A1000204_DMMETPARQUET/MET.parquet',
        '{x}/A100020/GEN_A1000205_DMDOMPARQUET/DOM.parquet'
        ),
    cind = c(
        '{x}/A100020/GEN_A1000202_DIMETPARQUET/MET.parquet',
        '{x}/A100020/GEN_A1000206_DIDOMPARQUET/DOM.parquet'
        ),
    clog = c(
        '{x}/A100020/GEN_A1000202_DMMETPARQUET/MET.parquet',
        '{x}/A100020/GEN_A1000206_DMDOMPARQUET/DOM.parquet'
        ),
    cfam = c(
        '{x}/A100020/GEN_A1000202_DFMETPARQUET/MET.parquet',
        '{x}/A100020/GEN_A1000206_DFDOMPARQUET/DOM.parquet'
        )
  ),
  "2021" = list(
    pind = c(
      '{x}/A100021/GEN_A1000214_DIMETAPARQUET/META.parquet',
      '{x}/A100021/GEN_A1000214_DIMETBPARQUET/METB.parquet',
      '{x}/A100021/GEN_A1000214_DIMETCPARQUET/METC.parquet',
      '{x}/A100021/GEN_A1000215_DIDOMPARQUET/DOM.parquet'
    ),
    plog = c(
      '{x}/A100021/GEN_A1000214_DMMETPARQUET/MET.parquet',
      '{x}/A100021/GEN_A1000215_DMDOMPARQUET/DOM.parquet'
    ),
    cind = c(
      '{x}/A100021/GEN_A1000212_DIMETPARQUET/MET.parquet',
      '{x}/A100021/GEN_A1000216_DIDOMPARQUET/DOM.parquet'
    ),
    clog = c(
      '{x}/A100021/GEN_A1000212_DMMETPARQUET/MET.parquet',
      '{x}/A100021/GEN_A1000216_DMDOMPARQUET/DOM.parquet'
    ),
    cfam = c(
      '{x}/A100021/GEN_A1000212_DFMETPARQUET/MET.parquet',
      '{x}/A100021/GEN_A1000216_DFDOMPARQUET/DOM.parquet'
    )
  )
)

paths = list(
  ls3 = list(
    "gen_root" = "s3://mad/insee",
    "edl_root" = "s3://insee/sern-div-exploitations-statistiques-rp/edl",
    "ear_root" = "s3://insee/sern-div-exploitations-statistiques-rp/ear/{x}"
  ),
  aus = list(
    "gen_root" = "W:/",
    "edl_root" = "X:/HAB-Pole-EDL-BasesRP",
    "ear_root" = "X:/HAB-MaD-SeRN/ear/{x}"
  ),
  pc = list(
    "gen_root" = "~/work/insee/rp/an={an}/{x}/",
    "edl_root" = "~/work/insee/rp/an={an}",
    "ear_root" = "~/work/insee/ear/{x}"
  )
)

#' Renvoie une liste des tables du RP21
#' @param conn : connexion duckdb, peut être obtenu par la fonction get_conn
#' @return Liste de 5 éléments nommés 'pind', 'plog', 'cind', 'clog', 'cfam'
#' selon principal (p)/complémentaire(c) et individu(ind)/logement(log)/famille(fam)
#' @examples
#' conn <- get_conn()
#' trp21 <- get_rp21(conn)
#' # population France
#' trp21$pind |>
#'   dplyr::count(wt = ipondi)
#' @export
get_rp <- function(conn, an, src = c("gen", "edl")) {
  src = match.arg(src)
  if (src == "edl" && site == "pc") {
    src = "gen"
  }
  # gen ----
  if (src == "gen") {
    root = paths[[site]]$gen_root
    if (site == "pc") {
      pat = gsub('{an}', an, root, fixed = T)
      paths = extend(rp_ext, pat)
    } else {
      paths = rp_gen_files[[as.character(an)]]
    }
  # edl ----
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
    root = paths[[site]]$edl_root
    paths = extend(
      cvt[rp_ext],
      paste0(root, "/RP", an2, "/PARQUET/{x}", angeo2, "/")
    )
    names(paths) = names(cvt)
  }
  tbl_list(conn, paths, lower = TRUE)
}

#' Extensions RP
#' @export
rp_ext = c("pind", "plog", "cind", "clog", "cfam")

#' Extensions EAR
#' @export
ear_ext = c("ind", "log", "fam", "liens")

#' Renvoie une liste des fichiers EAR
#' @param conn : connexion duckdb, peut être obtenu par la fonction get_conn
#' @return Liste de 4 éléments nommés 'ind', 'log', 'fam', 'liens' pour être passée
#' à `get_tbl` ou `get_ds` selon exemple ci-dessous
#' @examples
#' conn <- get_conn()
#' t_ear <- tbl_list(conn, ear_files(), level=1)
#' t_ear$ind |>
#'   dplyr::group_by(an) |>
#'   dplyr::count() |>
#'   dplyr::arrange(an) |>
#'   print()
#' @export
ear_files <- function() {
  extend(ear_ext, paths[[site]]$ear_root)
}
