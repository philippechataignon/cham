rp_gen_files = lapplyn(
  2020:2022,
  \(an) {
    an = an %% 100L
    list(
      pind = c(
        gen(glue::glue('A1000{an}/GEN_A1000{an}4_DIMETPARQUET/MET.parquet')),
        gen(glue::glue('A1000{an}/GEN_A1000{an}5_DIDOMPARQUET/DOM.parquet'))
      ),
      plog = c(
        gen(glue::glue('A1000{an}/GEN_A1000{an}4_DMMETPARQUET/MET.parquet')),
        gen(glue::glue('A1000{an}/GEN_A1000{an}5_DMDOMPARQUET/DOM.parquet'))
      ),
      cind = c(
        gen(glue::glue('A1000{an}/GEN_A1000{an}2_DIMETPARQUET/MET.parquet')),
        gen(glue::glue('A1000{an}/GEN_A1000{an}6_DIDOMPARQUET/DOM.parquet'))
      ),
      clog = c(
        gen(glue::glue('A1000{an}/GEN_A1000{an}2_DMMETPARQUET/MET.parquet')),
        gen(glue::glue('A1000{an}/GEN_A1000{an}6_DMDOMPARQUET/DOM.parquet'))
      ),
      cfam = c(
        gen(glue::glue('A1000{an}/GEN_A1000{an}2_DFMETPARQUET/MET.parquet')),
        gen(glue::glue('A1000{an}/GEN_A1000{an}6_DFDOMPARQUET/DOM.parquet'))
      )
    )
  }
)
# correction pour pind du RP21 en 3 morceaux !
rp_gen_files[["2021"]]$pind <- c(
      gen('A100021/GEN_A1000214_DIMETAPARQUET/META.parquet'),
      gen('A100021/GEN_A1000214_DIMETBPARQUET/METB.parquet'),
      gen('A100021/GEN_A1000214_DIMETCPARQUET/METC.parquet'),
      gen('A100021/GEN_A1000215_DIDOMPARQUET/DOM.parquet')
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
#'   dplyr::filter(dc < "975") |>
#'   dplyr::count(wt = ipondi)
#' @export
get_rp <- function(conn, an, src = c("gen", "edl", "misc"), verbose = FALSE) {
  src = match.arg(src)
  if (src == "edl" && site == "pc") {
    src = "gen"
  }
  # gen ----
  if (src == "gen") {
    if (site %in% c("ls3", "aus")) {
      files = rp_gen_files[[as.character(an)]]
    } else {
      stop("src non présente sur ce site")
    }
    # edl ----
  } else if (src == "edl") {
    if (site == "ls3") {
      files = extend(rp_ext, file.path(s3expl, glue::glue("edl/an={an}/{{x}}.parquet")))
    } else if (site == "aus") {
      an2 = an %% 100
      angeo2 = (an + 2) %% 100
      cvt = list(
        "pind" = "prin_ind",
        "plog" = "prin_log",
        "cind" = "compl_ind",
        "clog" = "compl_log",
        "cfam" = "compl_fam"
      )
      root = "X:/HAB-Pole-EDL-BasesRP"
      files = extend(
        cvt[rp_ext],
        paste0(root, "/RP", an2, "/PARQUET/{x}", angeo2, "/")
      )
      names(files) = names(cvt)
    } else {
      stop("Source edl non présente sur ce site")
    }
  } else if (src == "misc" && an == 2022) {
    files = extend(rp_ext, file.path(s3perso, "edl/RP22/{x}.parquet"))
  } else if (src == "misc" && an == 2021) {
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
attach_db <- function(conn, path, db) {
  if (missing(db)) {
    db = path
  }
  DBI::dbExecute(conn, paste("DETACH DATABASE IF EXISTS", db))
  DBI::dbExecute(
    conn,
    paste0(
      "ATTACH '",
      file.path(s3perso, "duckdb", paste0(path, ".duckdb")),
      "' as ",
      db
    )
  )
  db
}

#' Renvoie une liste des tables RP depuis base duckdb
#' @param conn : connexion duckdb, peut être obtenu par la fonction get_conn
#' @return Liste de 5 éléments nommés 'pind', 'plog', 'cind', 'clog', 'cfam'
#' selon principal (p)/complémentaire(c) et individu(ind)/logement(log)/famille(fam)
#' @export
get_rpdb <- function(conn, an, type="rp") {
  db = attach_db(conn, path=paste0(type, an))
  lapplyn(rp_ext, function(x) dplyr::tbl(conn, paste0(db, ".", x)))
}

#' Renvoie une liste des tables EAR depuis base duckdb
#' @param conn : connexion duckdb, peut être obtenu par la fonction get_conn
#' @return Liste de 4 éléments nommés 'ind', 'log', 'fam' et 'liens'
#' @export
get_eardb <- function(conn, an, db = "ear") {
  db = attach_db(conn, path=paste0(db, an))
  lapplyn(ear_ext, function(x) dplyr::tbl(conn, paste0(db, ".", x)))
}

#' Extensions RP
#' @export
rp_ext = c("pind", "plog", "cind", "clog", "cfam")

#' Extensions EAR
#' @export
ear_ext = c("ind", "log", "fam", "liens")
