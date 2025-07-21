rp_gen_files = list(
  "2020" = list(
    pind = c(
      '{root}/A100020/GEN_A1000204_DIMETPARQUET/MET.parquet',
      '{root}/A100020/GEN_A1000205_DIDOMPARQUET/DOM.parquet'
    ),
    plog = c(
      '{root}/A100020/GEN_A1000204_DMMETPARQUET/MET.parquet',
      '{root}/A100020/GEN_A1000205_DMDOMPARQUET/DOM.parquet'
    ),
    cind = c(
      '{root}/A100020/GEN_A1000202_DIMETPARQUET/MET.parquet',
      '{root}/A100020/GEN_A1000206_DIDOMPARQUET/DOM.parquet'
    ),
    clog = c(
      '{root}/A100020/GEN_A1000202_DMMETPARQUET/MET.parquet',
      '{root}/A100020/GEN_A1000206_DMDOMPARQUET/DOM.parquet'
    ),
    cfam = c(
      '{root}/A100020/GEN_A1000202_DFMETPARQUET/MET.parquet',
      '{root}/A100020/GEN_A1000206_DFDOMPARQUET/DOM.parquet'
    )
  ),
  "2021" = list(
    pind = c(
      '{root}/A100021/GEN_A1000214_DIMETAPARQUET/META.parquet',
      '{root}/A100021/GEN_A1000214_DIMETBPARQUET/METB.parquet',
      '{root}/A100021/GEN_A1000214_DIMETCPARQUET/METC.parquet',
      '{root}/A100021/GEN_A1000215_DIDOMPARQUET/DOM.parquet'
    ),
    plog = c(
      '{root}/A100021/GEN_A1000214_DMMETPARQUET/MET.parquet',
      '{root}/A100021/GEN_A1000215_DMDOMPARQUET/DOM.parquet'
    ),
    cind = c(
      '{root}/A100021/GEN_A1000212_DIMETPARQUET/MET.parquet',
      '{root}/A100021/GEN_A1000216_DIDOMPARQUET/DOM.parquet'
    ),
    clog = c(
      '{root}/A100021/GEN_A1000212_DMMETPARQUET/MET.parquet',
      '{root}/A100021/GEN_A1000216_DMDOMPARQUET/DOM.parquet'
    ),
    cfam = c(
      '{root}/A100021/GEN_A1000212_DFMETPARQUET/MET.parquet',
      '{root}/A100021/GEN_A1000216_DFDOMPARQUET/DOM.parquet'
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
get_rp <- function(conn, an, src = c("gen", "edl", "dmtr"), verbose = FALSE) {
  src = match.arg(src)
  if (src == "edl" && site == "pc") {
    src = "gen"
  }
  # gen ----
  if (src == "gen") {
    root = paths[[site]]$gen_root
    if (site %in% c("ls3", "aus")) {
      files = lapply(
        rp_gen_files[[as.character(an)]],
        function(x) gsub('{root}', root, x, fixed = T)
      )
    } else if (site == "pc") {
      pat = gsub('{an}', an, root, fixed = T)
      files = extend(rp_ext, pat)
    } else {
      stop("src non présente sur ce site")
    }
    # edl ----
  } else if (src == "edl") {
    if (!site %in% c("ls3", "aus")) {
      stop("src non présente sur ce site")
    } else if (site == "ls3" && an == 2022) {
      files = extend(rp_ext, file.path(s3expl, "edl/an=2022/{x}.parquet"))
    } else {
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
      files = extend(
        cvt[rp_ext],
        paste0(root, "/RP", an2, "/PARQUET/{x}", angeo2, "/")
      )
      names(files) = names(cvt)
    }
  } else if (src == "dmtr" && an == 2022) {
    files = extend(rp_ext, file.path(s3perso, "edl/RP22/{x}.parquet"))
  } else if (src == "dmtr" && an == 2021) {
    files = extend(rp_ext, file.path(s3expl, "rp_repond/rprepond_{x}.parquet"))
  }
  ret = tbl_list(conn, files, lower = TRUE, verbose = verbose)
  if (length(ret) == 0) {
    warning("Liste vide !")
  }
  ret
}

#' Renvoie une liste des tables RP depuis base duckdb
#' @param conn : connexion duckdb, peut être obtenu par la fonction get_conn
#' @return Liste de 5 éléments nommés 'pind', 'plog', 'cind', 'clog', 'cfam'
#' selon principal (p)/complémentaire(c) et individu(ind)/logement(log)/famille(fam)
#' @export
get_rpdb <- function(conn, an, db = "rp") {
  DBI::dbExecute(conn, paste("DETACH DATABASE IF EXISTS", db))
  DBI::dbExecute(
    conn,
    paste0(
      "ATTACH '",
      file.path(s3perso, "duckdb", paste0(db, an, ".duckdb")),
      "' as ",
      db
    )
  )
  lapplyn(rp_ext, function(x) dplyr::tbl(conn, paste0(db, ".", x)))
}

#' Renvoie une liste des tables RP depuis base duckdb
#' @param conn : connexion duckdb, peut être obtenu par la fonction get_conn
#' @return Liste de 5 éléments nommés 'pind', 'plog', 'cind', 'clog', 'cfam'
#' selon principal (p)/complémentaire(c) et individu(ind)/logement(log)/famille(fam)
#' @export
get_eardb <- function(conn, an, db = "ear") {
  DBI::dbExecute(conn, paste("DETACH DATABASE IF EXISTS", db))
  DBI::dbExecute(
    conn,
    paste0(
      "ATTACH '",
      file.path(s3perso, "duckdb", paste0(db, an, ".duckdb")),
      "' as ",
      db
    )
  )
  lapplyn(ear_ext, function(x) dplyr::tbl(conn, paste0(db, ".", x)))
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
