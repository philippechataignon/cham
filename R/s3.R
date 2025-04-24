#' Crée un filesystem s3 avec les variables environnement
#' @export
s3createfs <- function() {
  arrow::S3FileSystem$create(
    access_key = Sys.getenv('AWS_ACCESS_KEY_ID'),
    secret_key = Sys.getenv('AWS_SECRET_ACCESS_KEY'),
    session_token = Sys.getenv('AWS_SESSION_TOKEN'),
    endpoint_override = Sys.getenv('AWS_S3_ENDPOINT')
  )
}

#' Renvoit une chaine s3 depuis un chemin
#' @param path : chemin
#' @return chaîne chemin
#' @export
s3path <- function(path, bucket, prefix) {
  if (missing(bucket)) bucket = Sys.getenv("BUCKET")
  if (missing(prefix)) prefix = Sys.getenv("PREFIX")
  if (nchar(prefix) == 0) {
    ret = file.path(bucket, path)
  } else {
    ret = file.path(bucket, prefix, path)
  }
  ret
}

#' Renvoit un SubFileSystemTree
#' @param path : chemin
#' @note Peut être utilisé avec read_parquet, write_parquet
#'       et read_csv_arrow
#' @export
s3file <- function(path, fs, ...) {
  if (missing(fs)) fs = s3fs
  fs$path(s3path(path, ...))
}

#' Teste si path existe
#' @param path : chemin
#' @export
s3exists <- function(path, fs, ...) {
  if (missing(fs)) fs = s3fs
  fileinfo = fs$GetFileInfo(s3path(path, ...))
  fileinfo[[1]]$type != 0
}

#' Télécharge un fichier dans s3 si absent
#' @param url : url à télécharger
#' @param path : chemin s3 à créer, si absent prend le dernier nom de l'url
#' @param fs : filesystem, par défaut s3fs
#' @param force : force le téléchargement même si fichier déjà présent
#' @return Renvoit un SubTreeFileSystem utilisable avec `copy_files`
#' @export
s3download <- function(url, path, fs, force = FALSE) {
  if (missing(path)) path = file.path("download", basename(url))
  file = basename(path)
  if (force || !s3exists(path, fs = fs)) {
    # crée répertoire temporaire
    tmpdir = tempfile()
    dir.create(tmpdir)
    outfile = file.path(tmpdir, file)
    ret = download.file(url, outfile)
    cat("Write to s3", path, "\n")
    arrow::copy_files(tmpdir, s3file(path, fs = fs))
    unlink(tmpdir, recursive = T)
  } else {
    cat(path, "present", "\n")
  }
  s3file(path, fs = fs)
}
