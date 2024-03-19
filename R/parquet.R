#' Compression zstd pour write_parquet
#' @export
write_parquet <- function(dt, path, quiet=F,...)
{
  if (is.data.table(dt)) {
    dt = setDF(copy(dt))
  }
  if (!quiet) {
    if ("SubTreeFileSystem" %in% class(path)) {
      spath = path$base_path
    } else {
      spath = path
    }
    cat("Write s3", spath, "\n")
  }
  arrow::write_parquet(dt, path, ..., compression = "zstd")
}

