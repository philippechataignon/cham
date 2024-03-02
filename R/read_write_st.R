#' Écrit une table de classe sf vers stockage s3
#' @param st_table : table de class st
#' @param name : nom du fichier à écrire en format gpkg
#' @param s3path : chemin du stockage s3, par défaut `gpkg`
#' @export
write_s3_st <- function(st_table, name, s3path = "gpkg")
{
  st_table_name = deparse(substitute(st_table))
  if (missing(name)) {
    name = paste0(st_table_name, ".gpkg")
  }
  cat("Prepare writing table", st_table_name, "to s3path", file.path(s3path, name), "\n")
  tmpdir = tempfile()
  dir.create(tmpdir)
  outfile = file.path(tmpdir, name)
  sf::st_write(st_table, outfile, quiet=T)
  cat("Write to s3", file.path(s3path, name), "\n")
  arrow::copy_files(tmpdir, s3file(file.path(s3path)))
  unlink(tmpdir, recursive = T)
}

#' Lit une table de classe sf depuis stockage s3
#' @param name : nom du fichier à lire
#' @param s3path : chemin du stockage s3, par défaut `gpkg`
read_s3_st <- function(name, s3path="gpkg")
{
  cat("Read from s3", file.path(s3path, name), "\n")
  infile = s3copy(file.path(s3path, name))
  ret = sf::st_read(infile)
  unlink(infile)
  ret
}
