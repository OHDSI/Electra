#' Create a table to visualize the number of patients in the cohort per time period.
#'
#' @description Generate a table to plot the number of patients included in the
#' cohort per time window. The function also saves the results to PostgreSQL database.
#'
#' @param cohort_id atlas cohort id of the phenotype to be evaluated.
#' @param scratch.table space and table where the cohort is available.
#' @param conn database connection for querying data
#' @param postgres_conn PostgreSQL connection for saving results (optional)
#' @param postgres_table_name PostgreSQL table name for saving results (optional)
#' @param database_id database identifier for the analysis (optional)
#' @param schema_name PostgreSQL schema name (default: "phenotype_library")
#'
#' @details
#'
#' ## Required packages
#' * data.table
#' * DBI
#'
#' @author Luisa Martínez (08APR2025)
#' @export
#' @family time_dependent_analysis
#' @import data.table
patient_x_time <- function(cohort_id, scratch.table, conn, 
                         postgres_conn = NULL, postgres_table_name = NULL, database_id = NULL, 
                         schema_name = "phenotype_library") {
  checkmate::assert_numeric(cohort_id)
  checkmate::expect_string(
    scratch.table,
    pattern = "^[^.]+\\.[^.]+(\\.[^.]+)?$",
    info = "scratch argument requires format: scratch_space_name.table_name"
  )
  checkmate::assert_class(conn, "DatabaseConnectorJdbcConnection")


  complete <- data.table::as.data.table(DBI::dbGetQuery(
    conn,
    paste0("SELECT subject_id, cohort_start_date, cohort_end_date FROM ",
           scratch.table,
           " WHERE cohort_definition_id = ",
           cohort_id))
  )

  start_seq <- seq.Date(complete[, min(cohort_start_date)],
                        complete[, max(cohort_end_date)], by = "month")

  c_tbl <- NULL

  for (i in seq_along(start_seq)) {

    c_tbl <- c(c_tbl,
               nrow(complete[cohort_start_date <= start_seq[i] &
                               cohort_end_date > start_seq[i]]))

  }

  to_plot <- data.frame(year_month = start_seq,
                        total_count = as.numeric(c_tbl))

  # Save to PostgreSQL if connection and table name provided
  # Convert to data.table first
  to_plot <- data.table::as.data.table(to_plot)
  to_plot <- save_to_postgres(
    postgres_conn = postgres_conn,
    postgres_table_name = postgres_table_name,
    data_frame = to_plot,
    cohort_id = cohort_id,
    database_id = database_id,
    schema_name = schema_name,
    function_name = "patient_x_time"
  )

  return(to_plot)

}
