#' Connexion duckdb
#' @description
#' Cette classe représente une connexion duckdb et permet de travailler avec
#' l'attribut 'conn'
#' @examples
#' # Crée une connexion
#' db = duckdb_conn$new()
#' # Utilise la connexion
#' tbl(db$conn, sql("select 42 as value"))
#' # Teste connection
#' db$is_connected()
#' # Déconnexion
#' # Note : db n'est pas supprimé et db$conn créera une nouvelle connexion
#' # avec les mêmes paramètres
#' db$disconnect()
#' # Teste connection
#' db$is_connected()
#' # Utilise à nouveau la connexion
#' tbl(db$conn, sql("select 44 as value"))
#' # Teste connection
#' db$is_connected()
#' db$disconnect()
#' @export
duckdb_conn <- R6Class(
  "duckdb_conn",
  public = list(
    #' @description
    #' Crée un objet 'duckdb_conn'
    #' @param ext Extensions chargées 'core': httpfs+spatial, 'geo' core+h3
    #' 'none' = pas d'extension. 'none' par défaut
    #' @param dbdir Nom du la base duckdb, par défaut base stockée en mémoire
    #' @return Un objet 'duckdb_conn'
    initialize = function(ext = "none", dbdir = ":memory:") {
      stopifnot(is.character(dbdir), length(dbdir) == 1)
      stopifnot(is.character(ext), length(ext) == 1)
      ext <- match.arg(ext, c("none", "core", "geo"))
      private$pext <- ext
      private$pdbdir <- dbdir
    },
    #' @description
    #' Fonction 'print' dédiée
    #' @param ... Inutilisé
    print = function(...) {
      message("Connexion duckdb ", "ext:", private$pext, " dbdir:", private$pdbdir)
      if (self$is_connected())
        print(private$pconn)
      else
        message("Non connecté")
      invisible(self)
    },
    #' @description
    #' Connexion valide ?
    #' @return TRUE/FALSE
    is_connected = function() {
      if (is.null(private$pconn)) {
        ret = F
      } else {
        ret = DBI::dbIsValid(private$pconn)
      }
      ret
    },
    #' @description
    #' Renouvelle la connexion sans changer de duckdb_conn
    #' Attention: toutes les tables en mémoire sont perdues
    connect_new = function() {
      self$disconnect()
      private$pconn = get_conn(ext = private$pext, dbdir = private$pdbdir)
      invisible(self)
    },
    #' @description
    #' Déconnecte la connexion courante. Une nouvelle connexion sera créée
    #' au prochain appel de '$conn'
    #' Attention: toutes les tables en mémoire sont perdues
    disconnect = function() {
      if (self$is_connected())
        suppressWarnings(dbDisconnect(private$pconn))
    }
  ),
  private = list(
    pext = NULL,
    pdbdir = NULL,
    pconn = NULL,
    finalize = function() {
      self$disconnect()
    }
  ),
  active = list(
    #' @field ext Extensions chargées
    ext = function(value) {
      if (!missing(value)) {
        stop('ext ne peut pas être modifié. Utiliser duckdb_conn$new(ext=...)')
      }
      private$pext
    },
    #' @field dbdir Nom de la base duckdb
    dbdir = function(value) {
      if (!missing(value)) {
        stop('dbdir ne peut pas être modifié. Utiliser duckdb_conn$new(dbdir=...)')
      }
      private$pdbdir
    },
    #' @field conn Connexion duckdb
    conn = function() {
      if (!self$is_connected()) {
        self$connect_new()
      }
      private$pconn
    }
  )
)
