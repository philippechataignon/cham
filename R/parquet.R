#' Compression zstd pour write_parquet
#' @export
write_parquet <- function(dt, ...)
{
  if (is.data.table(dt)) {
    dt = setDF(copy(dt))
  }
  arrow::write_parquet(dt, ..., compression = "zstd")
}

