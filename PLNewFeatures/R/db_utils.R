#' get_redshift_conn
#'
#' @param server
#' @param user
#' @param password
#'
#' @returns
#' @export
#'
get_redshift_conn <- function(
    server = Sys.getenv("redshift_server"),
    drivers_dir = "drivers",
    user = Sys.getenv("redshift_username"),
    password = Sys.getenv("redshift_password"),
    reds_url = Sys.getenv("redshift_url"),
    reds_port = Sys.getenv("redshit_port")) {
  checkmate::assert_string(server, min.chars = 1, na.ok = TRUE)
  checkmate::assert_directory_exists(drivers_dir)
  checkmate::assert_string(user, min.chars = 1)
  checkmate::assert_string(password, min.chars = 1)

  conn <- DatabaseConnector::connect(
    dbms = "redshift",
    user = user,
    password = password,
    server = server,
    extraSettings = reds_url,
    port = reds_port,
    pathToDriver = drivers_dir
  )
}

#' validate_db_table_perf
#'
#' @param string
#'
#' @returns logical
#' @export
#'
validate_db_table_perf <- function(string) {
  if (!grepl("^[[:alnum:]_]+\\.[[:alnum:]_]+$", string)) {
    stop("Invalid database.table format", call. = FALSE)
  }
  invisible(TRUE)
}

#' get_databricks_conn
#'
#' @param drivers_dir
#' @param user
#' @param password
#' @param databricks_host
#' @param databricks_http_path
#' @param ...
#'
#' @returns
#' @export
#'
get_databricks_conn <- function(
    drivers_dir = "drivers",
    user = "token",
    password = Sys.getenv("DATABRICKS_TOKEN"),
    databricks_host = Sys.getenv("DATABRICKS_HOST"),
    databricks_http_path = Sys.getenv("DATABRICKS_HTTP_PATH"),
    ...
) {

  checkmate::assert_string(user, min.chars = 1)
  if (!nzchar(Sys.getenv("DATABRICKS_TOKEN"))) {
    stop("(get_databricks_conn) Env variable 'DATABRICKS_TOKEN' is not set.")
  }
  checkmate::assert_string(password, min.chars = 1)
  checkmate::assert_directory_exists(drivers_dir)
  checkmate::assert_string(databricks_host, min.chars = 1)
  checkmate::assert_string(databricks_http_path, min.chars = 1)

  conn_details <- get_databricks_conn_details(
    drivers_dir =drivers_dir,
    user = user,
    password = password,
    databricks_host = databricks_host,
    databricks_http_path = databricks_http_path,
    ...
  )

  conn <- DatabaseConnector::connect(conn_details)
  logger::log_success("Established connection to Databricks")
  conn
}

#' get_databricks_conn_details
#'
#' @param drivers_dir
#' @param user
#' @param password
#' @param databricks_host
#' @param databricks_http_path
#' @param ...
#'
#' @returns
#' @export
#'
get_databricks_conn_details <- function(
    drivers_dir = "drivers",
    user = "token",
    password = Sys.getenv("DATABRICKS_TOKEN"),
    databricks_host = Sys.getenv("DATABRICKS_HOST"),
    databricks_http_path = Sys.getenv("DATABRICKS_HTTP_PATH"),
    ...
) {
  checkmate::assert_string(user, min.chars = 1)
  if (!nzchar(Sys.getenv("DATABRICKS_TOKEN"))) {
    stop("(get_databricks_conn) Env variable 'DATABRICKS_TOKEN' is not set.")
  }
  checkmate::assert_string(password, min.chars = 1)
  checkmate::assert_directory_exists(drivers_dir)
  checkmate::assert_string(databricks_host, min.chars = 1)
  checkmate::assert_string(databricks_http_path, min.chars = 1)

  DatabaseConnector::createConnectionDetails(
    dbms = "spark",
    user = user,
    password = password,
    connectionString = paste(
      "jdbc:databricks://",
      databricks_host,
      ":443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=",
      databricks_http_path,
      ";EnableArrow=1;",
      sep = ''
    ),
    pathToDriver = drivers_dir,
    extraSettings = Sys.getenv("postgresql_url")
  )
}


#' table_exists_in_databricks
#'
#' @param conn
#' @param table
#'
#' @returns
#' @export
#'
table_exists_in_databricks <- function(conn, table) {
  checkmate::assert_string(table, min.chars = 3L)
  tryCatch({
    suppressMessages(DBI::dbGetQuery(conn, glue::glue("SELECT 1 FROM {table} LIMIT 1")))
    TRUE
  }, error = function(e) FALSE)
}


#' db_table_has_zero_rows
#'
#' @param conn
#' @param table
#'
#' @returns
#' @export
#'
db_table_has_zero_rows <- function(conn, table) {
  checkmate::assert_string(table, min.chars = 3L)
  tryCatch({
    res <- suppressMessages(DBI::dbGetQuery(conn, glue::glue("SELECT count(*) as n FROM {table}")))
    isTRUE(res$n[1] == 0)
  }, error = function(e) FALSE)
}



