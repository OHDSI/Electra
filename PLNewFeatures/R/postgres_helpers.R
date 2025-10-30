#' Helper functions for PostgreSQL operations
#'
#' @description Utility functions to eliminate code duplication in PostgreSQL operations
#' across time dependent and prevalence analysis functions.
#'
#' @param postgres_conn PostgreSQL database connection
#' @param postgres_table_name PostgreSQL table name (without schema)
#' @param schema_name PostgreSQL schema name (default: "phenotype_library")
#' @param data_frame data.frame/data.table to save to PostgreSQL
#' @param cohort_id cohort identifier
#' @param database_id database identifier
#' @param concept_id concept identifier (optional, for code_x_time table)
#' @param function_name name of the calling function (for logging)
#'
#' @details
#'
#' ## Required packages
#' * DBI
#' * data.table
#' * logger
#'
#' @author Luisa Martínez (08APR2025)
#' @family postgres_helpers

#' @export
#' @import DBI
#' @import data.table
save_to_postgres <- function(postgres_conn, postgres_table_name, data_frame, 
                            cohort_id, database_id, concept_id = NULL, 
                            schema_name = "phenotype_library", function_name = "unknown") {
  
  # Validate required parameters
  if (is.null(postgres_conn) || is.null(postgres_table_name) || is.null(database_id)) {
    return(data_frame)
  }
  
  # Add required columns to data frame
  data_frame[, cohort_id := cohort_id]
  data_frame[, database_id := database_id]
  if (!is.null(concept_id)) {
    data_frame[, concept_id := concept_id]
  }
  
  # Create full table name
  table_name <- paste0(schema_name, ".", postgres_table_name)
  
  # Check if table exists and handle deletion/creation
  if (DBI::dbExistsTable(postgres_conn, postgres_table_name)) {
    # Remove existing records
    delete_query <- build_delete_query(table_name, cohort_id, database_id, concept_id)
    tryCatch({
      DBI::dbExecute(postgres_conn, delete_query)
      logger::log_info(logger::skip_formatter(paste0("Successfully deleted existing records from table: ", table_name)))
    }, error = function(e) {
      logger::log_warn(logger::skip_formatter(paste0("Could not delete existing records: ", e$message)))
    })
  } else {
    # Create table without primary key constraint
    create_table(postgres_conn, table_name, postgres_table_name, data_frame, concept_id)
  }
  
  # Insert new records
  tryCatch({
    DBI::dbWriteTable(postgres_conn, name = table_name, value = data_frame, 
                     append = TRUE, row.names = FALSE)
    logger::log_info(logger::skip_formatter(paste0("Successfully saved ", function_name, " data to PostgreSQL table: ", postgres_table_name)))
  }, error = function(e) {
    logger::log_error(logger::skip_formatter(paste0("Failed to save ", function_name, " data to PostgreSQL: ", e$message)))
  })
  
  return(data_frame)
}

#' Build DELETE query based on available parameters
#' @keywords internal
build_delete_query <- function(table_name, cohort_id, database_id, concept_id = NULL) {
  base_query <- paste0("DELETE FROM ", table_name, 
                      " WHERE cohort_id = ", cohort_id, 
                      " AND database_id = '", database_id, "'")
  
  if (!is.null(concept_id)) {
    base_query <- paste0(base_query, " AND concept_id = '", concept_id, "'")
  }
  
  return(base_query)
}

#' Create table without primary key constraint (allows duplicate rows)
#' @keywords internal
create_table <- function(postgres_conn, table_name, postgres_table_name, data_frame, concept_id = NULL) {
  logger::log_info(logger::skip_formatter(paste0("Table does not exist, creating table: ", table_name)))
  
  tryCatch({
    # Create table with proper PostgreSQL column types
    DBI::dbCreateTable(postgres_conn, name = table_name, fields = data_frame)
    
    # No primary key constraint - allows duplicate rows
    logger::log_info(logger::skip_formatter(paste0("Successfully created table: ", table_name)))
    
  }, error = function(e) {
    logger::log_error(logger::skip_formatter(paste0("Failed to create table: ", e$message)))
  })
}
