#' get_database_codes
#'
#' @param selected_databases
#' @param PL_app_dbs
#'
#' @returns list
#' @export
#'
get_database_codes <- function(selected_databases, PL_app_dbs) {
  checkmate::assert_vector(selected_databases)
  checkmate::assert_data_frame(PL_app_dbs)

  multiple_databaseCodes <- character()
  multiple_dbCodes <- character()
  multiple_databaseCodes_count <- numeric()
  for (d in seq_along(selected_databases))
  {
    dbc <- selected_databases[d]
    selDb_row <- subset(PL_app_dbs, Name == dbc)
    sel_databaseCode <- selDb_row$db_code
    print(sel_databaseCode)
    multiple_databaseCodes <- c(multiple_databaseCodes, sel_databaseCode)
    multiple_dbCodes <- c(multiple_dbCodes, sel_databaseCode)
  }
  # Need this parameter for sqlite_path_multiple function (database codes separated by "_")
  multiple_databaseCodes <- paste(sort(multiple_databaseCodes), collapse = "_")
  # Need this parameter for combination_exists function (database codes separated by ",")
  multiple_dbCodes <- paste(sort(multiple_dbCodes), collapse = ",")
  loggit("INFO", paste("multiple_databaseCodes ", multiple_databaseCodes), app = "get_database_codes")
  loggit("INFO", paste("multiple_dbCodes ", multiple_dbCodes), app = "get_database_codes")
  loggit("INFO", paste("multiple_databaseCodes ", multiple_databaseCodes), app = "get_database_codes")

  multiple_databaseCodes_count <- length(selected_databases)
  loggit("INFO", paste("multiple_databaseCodes_count ", multiple_databaseCodes_count))

  list(
    multiple_databaseCodes = multiple_databaseCodes,
    multiple_dbCodes = multiple_dbCodes,
    multiple_databaseCodes_count = multiple_databaseCodes_count
  )
}


#' parse_multiple_databaseCodes
#'
#' @param multiple_databaseCodes
#' @param databases_csv
#'
#' @returns vector of databases
#' @export
#'
parse_multiple_databaseCodes <- function(multiple_databaseCodes, databases_csv = "PL_App_Databases.csv") {
  checkmate::assert_string(multiple_databaseCodes, min.chars = 1)
  checkmate::assert_file_exists(databases_csv, extension = "csv")
  PL_app_dbs <- utils::read.csv("PL_App_Databases.csv")

  out <- PL_app_dbs$db_code %>%
    purrr::keep(\(db) {
      grepl(db, multiple_databaseCodes, fixed = TRUE)
    })
  logger::log_info(glue::glue("(parse_multiple_databaseCodes) parsed {multiple_databaseCodes} to {toString(out, Inf)}"))
  out
}
