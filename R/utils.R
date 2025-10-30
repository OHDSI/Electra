#' `%&&%`
#'
#' @param x
#' @param y
#'
#' @return x or y
#' @export
#'
`%&&%` <- function(x, y) {
  if (!is.null(x)) y else NULL
}


#' get_cdm_schema
#'
#' @param selectedDatabase
#'
#' @returns CDM schema name
#' @export
#'
get_cdm_schema <- function(selectedDatabase, databases_csv = "PL_App_Databases.csv") {
  checkmate::assert_atomic(selectedDatabase)
  checkmate::assert_file_exists(databases_csv)

  PL_app_dbs <- utils::read.csv(databases_csv)

  checkmate::assert_names(
    names(PL_app_dbs),
    must.include = c(
      "Name",
      "db_code",
      "rHealth_ServerName",
      "Access",
      "Folder",
      "Scratch_Space_Name",
      "Schema"
    )
  )

  cdmDatabaseSchema <- PL_app_dbs %>%
    dplyr::filter(db_code == selectedDatabase) %>%
    dplyr::pull(Schema)

  if (!nzchar(cdmDatabaseSchema)) {
    stop(glue::glue("Schema for database '{selectedDatabase}' not found. Please review '{databases_csv}' and try again."))
  }

  logger::log_info("Selected CDM schema: ", cdmDatabaseSchema)
  return(cdmDatabaseSchema)
}


#' get_cohort_table_for_evaluation
#'
#' @returns table name used in Cohort evaluation
#' @export
#'
get_cohort_table_for_evaluation <- function() {
  "cohort"
}
