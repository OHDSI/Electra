#' Read prevalence changes data from PostgreSQL with filtering
#'
#' @description Read prevalence changes data from PostgreSQL database
#' with filtering by cohort_id and database_id. This function provides a unified
#' interface to retrieve prevalence changes analysis results.
#'
#' @param cohort_id cohort identifier to filter by (optional)
#' @param database_id database identifier to filter by (optional)
#' @param postgres_conn PostgreSQL connection object (optional, uses default if NULL)
#' @param schema_name PostgreSQL schema name (default: "phenotype_library")
#' @param table_name PostgreSQL table name (default: "prevalence_changes")
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
#' The prevalence_changes table includes the following columns:
#' - cohort_id: Cohort identifier
#' - database_id: Database identifier
#' - concept_id: Concept identifier
#' - concept_name: Concept name
#' - domain: Domain (Condition, Drug, Procedure, Device, Measurement, Observation)
#' - proportion_cohort: Proportion within the cohort (Pc)
#' - proportion_total_population: Proportion within the total population (Pt)
#' - pc_pt_ratio: Pc/Pt ratio
#'
#' @examples
#' \dontrun{
#' # Read all prevalence changes data
#' prevalence_data <- read_prevalence_changes()
#' 
#' # Read prevalence changes for specific cohort and database
#' prevalence_data <- read_prevalence_changes(cohort_id = 123, 
#'                                           database_id = "optum_ehr")
#' }
#'
#' @author Luisa Martínez (08APR2025)
#' @export
#' @family prevalence_changes_analysis
read_prevalence_changes <- function(cohort_id = NULL,
                                   database_id = NULL,
                                   postgres_conn = NULL,
                                   schema_name = "phenotype_library",
                                   table_name = "prevalence_changes") {
  
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
  
  # Add WHERE clause if conditions exist
  if (length(where_conditions) > 0) {
    base_query <- paste0(base_query, " WHERE ", paste(where_conditions, collapse = " AND "))
  }
  
  # Add ORDER BY clause
  base_query <- paste0(base_query, " ORDER BY pc_pt_ratio DESC")
  
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
        cohort_id = integer(),
        database_id = character(),
        concept_id = integer(),
        concept_name = character(),
        domain = character(),
        proportion_cohort = numeric(),
        proportion_total_population = numeric(),
        pc_pt_ratio = numeric()
      )
      
      return(empty_result)
    } else {
      logger::log_error("Failed to read from table ", table_name, ": ", e$message)
      stop("Database query failed: ", e$message)
    }
  })
}

#' Read prevalence changes data and format for display
#'
#' @description Read prevalence changes data from PostgreSQL and format it
#' for display in the Shiny application, matching the original format.
#'
#' @param cohort_id cohort identifier to filter by
#' @param database_id database identifier to filter by
#' @param postgres_conn PostgreSQL connection object (optional, uses default if NULL)
#' @param schema_name PostgreSQL schema name (default: "phenotype_library")
#' @param table_name PostgreSQL table name (default: "prevalence_changes")
#'
#' @return data.table formatted for display with original column names
#'
#' @details
#'
#' ## Required packages
#' * data.table
#' * DBI
#'
#' ## Output Format
#' Returns a data.table with the following columns (matching original format):
#' - Concept id: Concept identifier
#' - Concept Name: Concept name
#' - Domain: Domain
#' - Proportion within the cohort (Pc): Proportion within the cohort
#' - Proportion within the total population (Pt): Proportion within the total population
#' - Pc/Pt: Ratio of proportions
#'
#' @examples
#' \dontrun{
#' # Read and format prevalence changes data for display
#' formatted_data <- read_prevalence_changes_formatted(cohort_id = 123, 
#'                                                    database_id = "optum_ehr")
#' }
#'
#' @author Luisa Martínez (08APR2025)
#' @export
#' @family prevalence_changes_analysis
read_prevalence_changes_formatted <- function(cohort_id,
                                             database_id,
                                             postgres_conn = NULL,
                                             schema_name = "phenotype_library",
                                             table_name = "prevalence_changes") {
  
  checkmate::assert_numeric(cohort_id)
  checkmate::assert_string(database_id, min.chars = 1)
  
  # Read raw data from PostgreSQL
  raw_data <- read_prevalence_changes(
    cohort_id = cohort_id,
    database_id = database_id,
    postgres_conn = postgres_conn,
    schema_name = schema_name,
    table_name = table_name
  )
  
  # Format data to match original display format
  if (nrow(raw_data) > 0) {
    formatted_data <- raw_data %>%
      dplyr::select(
        "Concept id" = concept_id,
        "Concept Name" = concept_name,
        "Domain" = domain,
        "Proportion within the cohort (Pc)" = proportion_cohort,
        "Proportion within the total population (Pt)" = proportion_total_population,
        "Pc/Pt" = pc_pt_ratio
      ) %>%
      dplyr::mutate(dplyr::across(dplyr::all_of(
        c(
          "Proportion within the cohort (Pc)",
          "Proportion within the total population (Pt)",
          "Pc/Pt"
        )
      ), ~ round(.x, digits = 2)))
    
    return(formatted_data)
  } else {
    # Return empty data.table with correct structure
    empty_result <- data.table::data.table(
      "Concept id" = integer(),
      "Concept Name" = character(),
      "Domain" = character(),
      "Proportion within the cohort (Pc)" = numeric(),
      "Proportion within the total population (Pt)" = numeric(),
      "Pc/Pt" = numeric()
    )
    
    return(empty_result)
  }
}
