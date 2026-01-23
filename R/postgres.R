param_ear <- function(annee, num_base = "01")
{
  list(
    dbname = if (annee < 2025) {
      paste0("pi_pg_ear", annee, "_pd", num_base)
    } else {
      paste0("ci_pg_ear", annee, "_cl", num_base)
    },
    host = if (annee < 2025) {
      paste0("pdear", annee, "lg0", num_base)
    } else {
      paste0("clear", annee, "lg0", num_base)
    },
    port = 1983L
  )
}

#' Liste des sources de données RP simples (sans paramètre an)
#' @export
list_db = list(
  pl = list(dbname="ci_pg_popleg_cl01", host="clpopleglg001", port=1983L),
  pl_prod = list(dbname="pi_pg_rpplgarc_pd01", host="pdrpplgarclg001", port=1983L),
  pl_dev = list(dbname="di_pg_popleg_dv01", host="dvpopleglg001", port=1983L),
  achille = list(dbname="ci_pg_achille_cl01", host="clachillelg001", port=1983L),
  achille_dev = list(dbname="di_pg_achille_dv01", host="dvachilleldb01", port=1983L),
  rorcal = list(dbname="ci_pg_rorcal_cl01", host="clrorcallg001.ad.insee.intra", port=1983L),
  rca = list(dbname="ci_pg_rca_cl01", host="clrcalg001.ad.insee.intra", port=1983L),
  rorcal_dev = list(dbname="di_pg_rorcal_dv01", host="dvrorcallg001.ad.insee.intra", port=1983L),
  rca_dev = list(dbname="di_pg_rca_dv01", host="dvrcalg001.ad.insee.intra", port=1983L),
  omer = list(dbname="ci_pg_homere_cl01", host="clhomerelg001", port=1983L),
  omer_qf1 = list(dbname="di_pg_homere_dv01", host="dvhomerelg001", port=1983L),
  omer_qf2 = list(dbname="di_pg_homere_dv02", host="dvhomerelg002", port=1983L),
  odic= list(dbname="ci_pg_odic_cl01", host="clodiclg001", port=1983L),
  odic_qf1 = list(dbname="di_pg_odic_dv02", host="dvodiclg002", port=1983L),
  odic_qf2 = list(dbname="di_pg_odic_dv03", host="dvodiclg003", port=1983L),
  odic_dv2  = list(dbname="di_pg_odic_dv02", host="dvodiclg002", port=1983L),
  itac = list(dbname="ci_pg_itac_cl01", host="clitaclg001", port=1983L),
  p7_dv1 = list(dbname="di_pg_p7_dv01", host="dvp7lg001", port=1983L),
  p7mca_dv1 = list( dbname="di_pg_p7mca_dv01", host="dvp7mcalg001", port=1983L),
  p7mca = list(dbname="pi_pg_p7mca_pd01", host="pdp7mcalg001", port=1983L)
)

#' Renvoie une liste contenant les paramètres de connexion Postgres
#' @param source nom de la source RP, par exemple "omer" ou "ear"
#' @param an facultatif sauf pour certaines sources RP
#' @return Liste de 4 éléments nommés 'dbname', 'host', 'user' et 'port'
#' @export
db_param <- function(source, an=NULL)
{
  if (source=="ear") {
    if (is.null(an))
      stop("'an' doit être rensigné pour la source 'ear'")
    param = param_ear(an)
  } else {
    param = list_db[[source]]
  }

  if (Sys.getenv("USERNAME") == "onyxia") {
    param$user <- strsplit(Sys.getenv("KUBERNETES_NAMESPACE"), "-")[[1]][2]
  } else {
    param$user <- tolower(Sys.getenv("USERNAME"))
  }
  if (Sys.getenv("PASSWORDINSEE") == "") {
    Sys.setenv(PASSWORDINSEE = rstudioapi::askForPassword("Mot de passe"))
  }
  param
}

#' Renvoie une connexion Postgres à partir dune liste créée par [db_param()]
#' @param param liste de paramètres créée par [db_param()]
#' @return Connexion Postgres
#' @examples
#' conn_pg <- pg_connect(db_param("rorcal_clone"))
#' conn_ear2025 <- pg_connect(db_param("ear", an=2025))
#' @export
pg_connect <- function(param)
{
  DBI::dbConnect(
    RPostgres::Postgres(),
    dbname=param$dbname,
    host = param$host,
    port = param$port,
    user = param$user,
    password=Sys.getenv("PASSWORDINSEE")
  )
}

#' Attache une base Postgres à duckdb à partir dune liste créée par [db_param()]
#' @param conn connexion duckdb, peut être obtenu par la fonction get_conn
#' @param param liste de paramètres créée par [db_param()]
#' @param db nom de l'alias de la base
#' @param schema nom du schema Postgres, 'public' par défaut
#' @return Code retour dbExecute
#' @examples
#' conn = DBI::dbConnect(duckdb::duckdb())
#' pg_attach(conn, db_param("ear", 2025), db="ear", schema = "rp")
#' pg_attach(conn, db_param("rorcal_clone"), db="rorcal")
# tbl(conn, "ear.individus")
# tbl_rorcal = tbl(conn, "rorcal.entite_adressee")
#' @export
pg_attach <- function(conn, param, db = "pg", schema = "public") {
  conn_str = glue::glue("
    host={param$host} port={param$port} user={param$user}
    password={Sys.getenv('PASSWORDINSEE')} dbname={param$dbname}
  ")
  DBI::dbExecute(conn, paste("DETACH DATABASE IF EXISTS", db))
  DBI::dbExecute(
    conn,
    glue::glue("
      INSTALL postgres;
      LOAD postgres;
      ATTACH '{conn_str}' AS {db} (TYPE postgres, SCHEMA '{schema}', READ_ONLY);
    ")
  )
}

#' Déconnecte toutes les connexions Postgres
#' @export
disconnect_pg <- function() {
  for (o in (ls(envir = globalenv()))) {
    if (is(get(o), 'PqConnection')) {
      cat("Disconnect: ", o, "\n")
      DBI::dbDisconnect(get(o))
      rm(list=paste(o), envir = globalenv())
    }
  }
}
