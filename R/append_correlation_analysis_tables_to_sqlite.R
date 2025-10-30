#' append_correlation_analysis_tables_to_sqlite
#'
#' @param exportFolder
#' @param sqliteDbPath
#'
#' @returns vector
#' @export
#'
append_correlation_analysis_tables_to_sqlite <- function(exportFolder, sqliteDbPath, cohort_id = "") {
  checkmate::assert_directory_exists(exportFolder)
  checkmate::assert_file_exists(sqliteDbPath)

  table_names <- get_correlation_analysis_output_table_names(cohort_id)

  conn_sqlite <- DBI::dbConnect(RSQLite::SQLite(), sqliteDbPath)
  on.exit(DBI::dbDisconnect(conn_sqlite))

  if (grepl("__COMMA__", sqliteDbPath)) {
    logger::log_warn(glue::glue("(append_correlation_analysis_tables_to_sqlite) Removing '__COMMA__' from '{sqliteDbPath}'"))
    sqliteDbPath <- gsub("__COMMA__", ",", sqliteDbPath)
  }

  table_names %>%
    sapply(function(table) {
      logger::log_info(glue::glue("(append_correlation_analysis_tables_to_sqlite) Writing table '{table}' to {sqliteDbPath}"))
      csv_path <- file.path(exportFolder, paste0(table, ".csv"))
      if (file.exists(csv_path)) {
        df <- readr::read_csv(csv_path, show_col_types = FALSE)
        DBI::dbWriteTable(conn_sqlite, table, df, overwrite = TRUE)
        logger::log_success(glue::glue("(append_correlation_analysis_tables_to_sqlite) Table '{table}' appended to {sqliteDbPath}"))
        TRUE
      } else {
        logger::log_warn(glue::glue("(append_correlation_analysis_tables_to_sqlite) Table '{csv_path}' does not exist."))
        FALSE
      }
    })
}
