#' @export
write_duckdb_parquet_raw <- function(
  conn,
  query,
  path,
  partition = "",
  verbose = F
) {
  cmd = glue::glue(
    "COPY ({query}) TO '{path}'
    (FORMAT 'parquet', COMPRESSION 'zstd' {partition})"
  )
  if (verbose) {
    cat(cmd, "\n")
  }
  DBI::dbExecute(conn, cmd)
  path
}

#' Écrit fichier parquet depuis une table duckdb
#' @param table table duckdb
#' @param path nom du fichier ou répertoire si partition est renseigné, par défaut le nom de la table
#' @param dir répertoire de sortie, éventuellement s3
#' @param partition chaine de la forme "var1, var2" pour créer une partition, pas de partition par défaut
#' @param keep Si TRUE, renvoit la table fournie en entrée sinon la table liée au fichier parquet créé. FALSE par défaut,
#' @param verbose indique la requete SQL générée
#' @return table dplyr liée au fichier parquet créé (voir paramètre 'keep' pour conserver la table en entrée)
#' @examples
#' s3expl = "s3://insee/sern-div-exploitations-statistiques-rp/ear_rp"
#' # Fichier simple ear_fam.parquet dans répertoire dir sous s3
#' write_duckdb_parquet(ear_fam, dir = s3expl)
#' # Fichier partitionné dans répertoire test_fam
#' write_duckdb_parquet(ear_fam,  dir = s3expl,
#'   filename="test_fam", partition="c_annee_col", verbose=T)
#' @export

write_duckdb_parquet <- function(
  table,
  path = NULL,
  dir = NULL,
  partition = NULL,
  keep = FALSE,
  verbose = FALSE
) {
  if (!is.null(dir)) {
    if (is.null(path)) {
      if (is.null(partition)) {
        path = file.path(dir, paste0(deparse(substitute(table)), ".parquet"))
      } else {
        path = file.path(dir, deparse(substitute(table)))
      }
    } else {
      path = file.path(dir, path)
    }
  }
  if (is.null(partition)) {
    partition_str = ""
  } else {
    partition_str = glue::glue(
      ", PARTITION_BY ({partition}), OVERWRITE_OR_IGNORE"
    )
  }
  write_duckdb_parquet_raw(
    conn = table$src$con,
    query = sql_render(table),
    path = path,
    partition = partition_str,
    verbose = verbose
  )
  if (keep)
    table
  else
    tbl_pqt(table$src$con, path)
}
