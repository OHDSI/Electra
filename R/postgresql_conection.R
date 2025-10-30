#' get_postgresql_connection_details
#'
#' Set the Read-Write connection details
#'
#' @return
#' @export
#'
#' @examples
get_postgresql_connection_details <- function() {
  user <- Sys.getenv("postgresql_username")
  password <- Sys.getenv("postgresql_password")
  checkmate::assert_string(user, min.chars = 1)
  checkmate::assert_string(password, min.chars = 1)

  DatabaseConnector::createConnectionDetails(
    dbms = "postgresql",
    server = Sys.getenv("postgresql_url"),
    extraSettings = "",
    port = 5432,
    pathToDriver = paste(getwd(), "drivers", sep = "/"),
    user = user,
    password = password
  )
}


#' get_postgresql_RO_connection_details
#'
#' Set the READ-ONLY connection details
#'
#' @return
#' @export
#'
get_postgresql_RO_connection_details <- function() {
  user <- Sys.getenv("postgresql_RO_username")
  password <- Sys.getenv("postgresql_RO_password")

  checkmate::assert_string(user, min.chars = 1)
  checkmate::assert_string(password, min.chars = 1)

  DatabaseConnector::createConnectionDetails(
    dbms = "postgresql",
    server = Sys.getenv("postgresql_url"),
    extraSettings = "",
    port = 5432,
    pathToDriver = paste(getwd(), "drivers", sep = "/"),
    user = user,
    password = password
  )
}


#' run_query
#'
#' @param query_sql
#'
#' @return
#' @export
#'
run_query <- function(query_sql) {
  checkmate::assert_string(query_sql, min.chars = 1)
  connection <- get_postgresql_RO_connection_details()
  con <- DatabaseConnector::connect(connection)
  tryCatch({
    return(DBI::dbGetQuery(con, query_sql))
  }, error = function() {
    logger::log_error("Failed to execute query:\n", sql_home_page)
  },
  finally = {
    DatabaseConnector::disconnect(con)
  })
}

#' run_query_RW
#'
#' @param query_sql
#'
#' @return
#' @export
#'
run_query_RW <- function(query_sql) {
  connection <- get_postgresql_connection_details()
  con <- DatabaseConnector::connect(connection)
  tryCatch({
    return(DBI::dbGetQuery(con, query_sql))
  }, finally = {
    DatabaseConnector::disconnect(con)
  })
}

#' execute_command
#'
#' Connect to redshift and only execute an insert without expecting a result set
#'
#' @param query_sql
#'
#' @return
#' @export
#'
execute_command <- function(query_sql) {
  connection <- get_postgresql_connection_details()
  con <- DatabaseConnector::connect(connection)
  tryCatch({
    return(DBI::dbExecute(con, query_sql))
  }, finally = {
    DatabaseConnector::disconnect(con)
  })
}
