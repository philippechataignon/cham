#' Écrit fichier parquet depuis une table duckdb
#' @param conn : connexion duckdb
#' @param name : nom de la table
#' @param filename : nom du fichier ou répertoire si partition est renseigné, par défaut `name`
#' @param dir : répertoire de sortie, éventuellement s3
#' @param order_by : ordre éventuel de sortie sous forme de vecteur character
#' @param partition : chaine de la forme "var1, var2" pour créer une partition, pas de partition par défaut
#' @param verbose : indique la requete SQL générée
#' @return nom du fichier/répertoire
#' @examples
#' s3expl = "s3://insee/sern-div-exploitations-statistiques-rp/ear_rp"
#' # Fichier simple ear_fam.parquet dasn répertoire dir sous s3
#' outfile = write_duckdb_parquet(conn = conn, name="ear_fam", dir = s3expl)
#' # Fichier simple ear_fam.parquet trié dans répertoire courant
#' outfile = write_duckdb_parquet(conn = conn, name="ear_fam", order_by=c("var1", "var2"))
#' # Fichier partitionné dans répertoire test_fam
#' outfile = write_duckdb_parquet(conn, name="ear_fam",  dir = s3expl,
#'   filename="test_fam", partition="c_annee_col", verbose=T)
#' @export
write_duckdb_parquet <- function(
  conn,
  name = NULL,
  filename = NULL,
  dir = ".",
  partition = NULL,
  order_by = NULL,
  verbose = FALSE
) {
  if (is.null(filename)) {
    filename = file.path(dir, paste0(name, ".parquet"))
  } else {
    filename = file.path(dir, filename)
  }
  if (is.null(partition)) {
    partition_str = ""
  } else {
    partition_str = glue::glue(
      ", PARTITION_BY ({partition}), OVERWRITE_OR_IGNORE"
    )
  }
  if (is.null(order_by)) {
    order_by = ""
  } else {
    order_by = paste(" ORDER BY", paste0(order_by, collapse = ','))
  }

  cmd = glue::glue(
    "COPY (FROM {name} {order_by}) TO '{filename}'
    (FORMAT 'parquet', COMPRESSION 'zstd' {partition_str})"
  )
  if (verbose) cat(cmd, "\n")
  dbExecute(conn, cmd)
  filename
}
