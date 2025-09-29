conn = dbConnect(duckdb::duckdb())
dir = "/home/onyxia/extensions"
system(glue("gunzip {dir}/*"))
for (ext in c("httpfs", "spatial", "h3")) {
  dbExecute(conn, glue("install '{dir}/{ext}.duckdb_extension'"))
}
