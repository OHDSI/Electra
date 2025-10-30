#' Read time dependent analysis tables from PostgreSQL with filtering
#'
#' @description Read time dependent analysis tables from PostgreSQL database
#' with filtering by cohort_id and database_id. This function provides a unified
#' interface to retrieve data from the four main time dependent analysis tables.
#'
#' @param table_name PostgreSQL table name to read from. Options:
#'   - "patient_eofup" - End of follow-up analysis
#'   - "patient_recruitment" - Patient recruitment analysis  
#'   - "patient_x_time" - Patient count over time
#'   - "code_x_time" - Code frequency over time
#' @param cohort_id cohort identifier to filter by (optional)
#' @param database_id database identifier to filter by (optional)
#' @param concept_id concept identifier to filter by (only for code_x_time table, optional)
#' @param postgres_conn PostgreSQL connection object (optional, uses default if NULL)
#' @param schema_name PostgreSQL schema name (default: "phenotype_library")
#'
#' @return data.table with filtered results
#'
#' @details
#'
#' ## Required packages
#' * data.table
#' * DBI
#'
#' ## Table Structure
#' All tables include the following columns:
#' - year_month: Date of the time period
#' - total_count: Count for that time period
#' - cohort_id: Cohort identifier
#' - database_id: Database identifier
#' - concept_id: Concept identifier (only for code_x_time table)
#'
#' @examples
#' \dontrun{
#' # Read all patient recruitment data
#' recruitment_data <- read_time_dependent_table("patient_recruitment")
#' 
#' # Read patient recruitment for specific cohort and database
#' recruitment_data <- read_time_dependent_table("patient_recruitment", 
#'                                               cohort_id = 123, 
#'                                               database_id = "optum_ehr")
#' 
#' # Read code frequency for specific concept
#' code_data <- read_time_dependent_table("code_x_time", 
#'                                       cohort_id = 123, 
#'                                       database_id = "optum_ehr",
#'                                       concept_id = 456)
#' }
#'
#' @author Luisa Martínez (08APR2025)
#' @export
#' @family time_dependent_analysis
read_time_dependent_table <- function(table_name,
                                     cohort_id = NULL,
                                     database_id = NULL,
                                     concept_id = NULL,
                                     postgres_conn = NULL,
                                     schema_name = "phenotype_library") {
  
  # Validate table name
  valid_tables <- c("patient_eofup", "patient_recruitment", "patient_x_time", "code_x_time")
  checkmate::assert_choice(table_name, valid_tables)
  
  # Validate concept_id is only used with code_x_time table
  if (!is.null(concept_id) && table_name != "code_x_time") {
    stop("concept_id parameter can only be used with 'code_x_time' table")
  }
  
  # Get PostgreSQL connection if not provided
  if (is.null(postgres_conn)) {
    postgres_conn <- DatabaseConnector::connect(get_postgresql_connection_details())
    on.exit(DBI::dbDisconnect(postgres_conn), add = TRUE)
  }
  
  # Build the base query
  base_query <- paste0("SELECT * FROM ", schema_name, ".", table_name)
  
  # Build WHERE conditions
  where_conditions <- character(0)
  
  if (!is.null(cohort_id)) {
    where_conditions <- c(where_conditions, paste0("cohort_id = ", cohort_id))
  }
  
  if (!is.null(database_id)) {
    where_conditions <- c(where_conditions, paste0("database_id = '", database_id, "'"))
  }
  
  if (!is.null(concept_id) && table_name == "code_x_time") {
    where_conditions <- c(where_conditions, paste0("concept_id = '", concept_id, "'"))
  }
  
  # Add WHERE clause if conditions exist
  if (length(where_conditions) > 0) {
    base_query <- paste0(base_query, " WHERE ", paste(where_conditions, collapse = " AND "))
  }
  
  # Add ORDER BY clause
  base_query <- paste0(base_query, " ORDER BY year_month")
  
  # Execute query
  tryCatch({
    result <- DBI::dbGetQuery(postgres_conn, base_query)
    result <- data.table::as.data.table(result)
    
    logger::log_info("Successfully read ", nrow(result), " rows from table: ", table_name)
    
    return(result)
    
  }, error = function(e) {
    # If table doesn't exist or query fails, return empty data.table with expected structure
    if (grepl("relation.*does not exist", e$message, ignore.case = TRUE) || 
        grepl("table.*does not exist", e$message, ignore.case = TRUE)) {
      logger::log_warn("Table ", table_name, " does not exist in PostgreSQL. Returning empty result.")
      
      # Create empty data.table with expected columns
      empty_result <- data.table::data.table(
        year_month = as.Date(character()),
        total_count = numeric()
      )
      
      # Add cohort_id and database_id columns if they were requested
      if (!is.null(cohort_id)) {
        empty_result[, cohort_id := integer()]
      }
      if (!is.null(database_id)) {
        empty_result[, database_id := character()]
      }
      if (!is.null(concept_id) && table_name == "code_x_time") {
        empty_result[, concept_id := character()]
      }
      
      return(empty_result)
    } else {
      logger::log_error("Failed to read from table ", table_name, ": ", e$message)
      stop("Database query failed: ", e$message)
    }
  })
}

#' Read all time dependent analysis tables for a specific cohort and database
#'
#' @description Convenience function to read all four time dependent analysis tables
#' for a specific cohort and database combination.
#'
#' @param cohort_id cohort identifier to filter by
#' @param database_id database identifier to filter by
#' @param postgres_conn PostgreSQL connection object (optional, uses default if NULL)
#' @param schema_name PostgreSQL schema name (default: "phenotype_library")
#' @param concept_ids concept identifiers to filter code_x_time table by (optional)
#'
#' @return list containing data.tables for each analysis type:
#'   - patient_eofup: End of follow-up analysis
#'   - patient_recruitment: Patient recruitment analysis
#'   - patient_x_time: Patient count over time
#'   - code_x_time: Code frequency over time
#'
#' @details
#'
#' ## Required packages
#' * data.table
#' * DBI
#'
#' @examples
#' \dontrun{
#' # Read all time dependent analysis data for a cohort
#' all_data <- read_all_time_dependent_tables(cohort_id = 123, 
#'                                           database_id = "optum_ehr")
#' 
#' # Read with specific concept IDs for code_x_time table
#' all_data <- read_all_time_dependent_tables(cohort_id = 123, 
#'                                           database_id = "optum_ehr",
#'                                           concept_ids = c("12345", "67890"))
#' 
#' # Access individual tables
#' recruitment_data <- all_data$patient_recruitment
#' eofup_data <- all_data$patient_eofup
#' code_data <- all_data$code_x_time
#' }
#'
#' @author Luisa Martínez (08APR2025)
#' @export
#' @family time_dependent_analysis
read_all_time_dependent_tables <- function(cohort_id,
                                          database_id,
                                          postgres_conn = NULL,
                                          schema_name = "phenotype_library",
                                          concept_ids = NULL) {
  
  checkmate::assert_numeric(cohort_id)
  checkmate::assert_string(database_id, min.chars = 1)
  
  # Get PostgreSQL connection if not provided
  if (is.null(postgres_conn)) {
    postgres_conn <- DatabaseConnector::connect(get_postgresql_connection_details())
    on.exit(DBI::dbDisconnect(postgres_conn), add = TRUE)
  }
  
  # Read all four tables
  tables <- list(
    patient_eofup = read_time_dependent_table("patient_eofup", 
                                             cohort_id = cohort_id, 
                                             database_id = database_id,
                                             postgres_conn = postgres_conn,
                                             schema_name = schema_name),
    patient_recruitment = read_time_dependent_table("patient_recruitment", 
                                                   cohort_id = cohort_id, 
                                                   database_id = database_id,
                                                   postgres_conn = postgres_conn,
                                                   schema_name = schema_name),
    patient_x_time = read_time_dependent_table("patient_x_time", 
                                              cohort_id = cohort_id, 
                                              database_id = database_id,
                                              postgres_conn = postgres_conn,
                                              schema_name = schema_name),
    code_x_time = read_time_dependent_table("code_x_time", 
                                           cohort_id = cohort_id, 
                                           database_id = database_id,
                                           concept_id = concept_ids,
                                           postgres_conn = postgres_conn,
                                           schema_name = schema_name)
  )
  
  logger::log_info("Successfully read all time dependent analysis tables for cohort_id: ", 
                  cohort_id, " and database_id: ", database_id)
  
  return(tables)
}
