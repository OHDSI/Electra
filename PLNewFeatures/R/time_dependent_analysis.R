#' Run time dependent analysis for a cohort and save results to PostgreSQL
#'
#' @description This function runs all four time dependent analysis functions
#' (patient_eofup, patient_recruitment, patient_x_time, code_x_time) for a given
#' cohort and saves the results to PostgreSQL database. This function is designed
#' to be called during cohort evaluation to populate the database with time
#' dependent analysis results.
#'
#' @param cohort_id cohort identifier to analyze
#' @param scratch.table space and table where the cohort is available
#' @param cdm_schema general database cdm schema
#' @param concept_sets data frame containing concept sets information
#' @param conn database connection for querying data
#' @param postgres_conn PostgreSQL connection for saving results
#' @param database_id database identifier for the analysis
#' @param server server name for logging
#' @param drivers_dir directory containing database drivers
#' @param max_cores maximum number of cores to use for parallel processing
#' @param schema_name PostgreSQL schema name (default: "phenotype_library")
#'
#' @return list containing results from all four time dependent analysis functions
#'
#' @details
#'
#' ## Required packages
#' * data.table
#' * DBI
#' * PLNewFeatures
#'
#' ## Functions Called
#' - patient_eofup: End of follow-up analysis
#' - patient_recruitment: Patient recruitment analysis
#' - patient_x_time: Patient count over time
#' - code_x_time: Code frequency over time (for each concept in concept_sets)
#'
#' ## PostgreSQL Tables Created/Updated
#' - patient_eofup: End of follow-up analysis results
#' - patient_recruitment: Patient recruitment analysis results
#' - patient_x_time: Patient count over time results
#' - code_x_time: Code frequency over time results
#'
#' @examples
#' \dontrun{
#' # Run time dependent analysis for a cohort
#' results <- time_dependent_analysis(
#'   cohort_id = 123,
#'   scratch.table = "scratch.cohort_table",
#'   cdm_schema = "cdm_schema",
#'   concept_sets = concept_sets_df,
#'   conn = redshift_conn,
#'   postgres_conn = postgres_conn,
#'   database_id = "optum_ehr",
#'   server = "server_name",
#'   drivers_dir = "drivers",
#'   max_cores = 4
#' )
#' }
#'
#' @author Luisa Martínez (08APR2025)
#' @export
#' @family time_dependent_analysis
time_dependent_analysis <- function(cohort_id,
                                   scratch.table,
                                   cdm_schema,
                                   concept_sets,
                                   conn,
                                   postgres_conn,
                                   database_id,
                                   server,
                                   drivers_dir,
                                   max_cores = 1,
                                   schema_name = "phenotype_library") {
  
  checkmate::assert_numeric(cohort_id)
  checkmate::assert_string(scratch.table, min.chars = 1)
  checkmate::assert_string(cdm_schema, min.chars = 1)
  checkmate::assert_data_frame(concept_sets)
  checkmate::assert_class(conn, "DatabaseConnectorJdbcConnection")
  checkmate::assert_class(postgres_conn, "DatabaseConnectorJdbcConnection")
  checkmate::assert_string(database_id, min.chars = 1)
  checkmate::assert_string(server, min.chars = 1, na.ok = TRUE)
  checkmate::assert_directory_exists(drivers_dir)
  checkmate::assert_numeric(max_cores, lower = 1)
  
  logger::log_info("Starting time dependent analysis for cohort_id: ", cohort_id, 
                  " and database_id: ", database_id)
  
  # Initialize results list
  results <- list()
  
  # Run patient_eofup analysis
  logger::log_info("Running patient_eofup analysis...")
  tryCatch({
    eofup_result <- patient_eofup(
      cohort_id = cohort_id,
      scratch.table = scratch.table,
      conn = conn,
      postgres_conn = postgres_conn,
      postgres_table_name = "patient_eofup",
      database_id = database_id,
      schema_name = schema_name
    )
    results$patient_eofup <- eofup_result
    logger::log_success("patient_eofup analysis completed successfully")
  }, error = function(e) {
    logger::log_error("Error in patient_eofup analysis: ", e$message)
    results$patient_eofup <- NULL
  })
  
  # Run patient_recruitment analysis
  logger::log_info("Running patient_recruitment analysis...")
  tryCatch({
    recruitment_result <- patient_recruitment(
      cohort_id = cohort_id,
      scratch.table = scratch.table,
      conn = conn,
      postgres_conn = postgres_conn,
      postgres_table_name = "patient_recruitment",
      database_id = database_id,
      schema_name = schema_name
    )
    results$patient_recruitment <- recruitment_result
    logger::log_success("patient_recruitment analysis completed successfully")
  }, error = function(e) {
    logger::log_error("Error in patient_recruitment analysis: ", e$message)
    results$patient_recruitment <- NULL
  })
  
  # Run patient_x_time analysis
  logger::log_info("Running patient_x_time analysis...")
  tryCatch({
    patient_x_time_result <- patient_x_time(
      cohort_id = cohort_id,
      scratch.table = scratch.table,
      conn = conn,
      postgres_conn = postgres_conn,
      postgres_table_name = "patient_x_time",
      database_id = database_id,
      schema_name = schema_name
    )
    results$patient_x_time <- patient_x_time_result
    logger::log_success("patient_x_time analysis completed successfully")
  }, error = function(e) {
    logger::log_error("Error in patient_x_time analysis: ", e$message)
    results$patient_x_time <- NULL
  })
  
  # Run code_x_time analysis for each concept
  logger::log_info("Running code_x_time analysis for concepts...")
  code_x_time_results <- list()
  
  if (nrow(concept_sets) > 0) {
    # Handle both column naming conventions (renamed vs original)
    if ("Id" %in% names(concept_sets) && "Domain" %in% names(concept_sets) && "Name" %in% names(concept_sets)) {
      # Data has renamed columns, select and rename them
      unique_concepts <- concept_sets %>%
        dplyr::select(concept_id = Id, concept_name = Name, domain_id = Domain) %>%
        dplyr::distinct_all()
    } else {
      # Data has original column names
      unique_concepts <- concept_sets %>%
        dplyr::select(concept_id, concept_name, domain_id) %>%
        dplyr::distinct_all()
    }
    
    logger::log_info("Found ", nrow(unique_concepts), " unique concepts to analyze")
    
    # Process concepts in parallel if max_cores > 1
    if (max_cores > 1 && nrow(unique_concepts) > 1) {
      logger::log_info("Processing concepts in parallel with ", max_cores, " cores")
      
      # Set up parallel processing
      future::plan(future::multisession, workers = max_cores)
      
      code_x_time_results <- unique_concepts %>%
        dplyr::mutate(
          result = furrr::future_pmap(
            list(concept_id, concept_name, domain_id),
            function(concept_id, concept_name, domain_id) {
              tryCatch({
                logger::log_info("Processing concept_id: ", concept_id, " (", concept_name, ")")
                result <- code_x_time(
                  concept_id = concept_id,
                  cdm_schema = cdm_schema,
                  domain = domain_id,
                  conn = conn,
                  postgres_conn = postgres_conn,
                  postgres_table_name = "code_x_time",
                  cohort_id = cohort_id,
                  database_id = database_id,
                  schema_name = schema_name
                )
                return(result)
              }, error = function(e) {
                logger::log_error("Error processing concept_id ", concept_id, ": ", e$message)
                return(NULL)
              })
            },
            .options = furrr::furrr_options(seed = TRUE)
          )
        ) %>%
        dplyr::filter(!is.null(result)) %>%
        dplyr::select(concept_id, concept_name, domain_id, result)
      
      # Reset future plan
      future::plan(future::sequential)
      
    } else {
      # Process concepts sequentially
      for (i in seq_len(nrow(unique_concepts))) {
        concept_row <- unique_concepts[i, ]
        concept_id <- concept_row$concept_id
        concept_name <- concept_row$concept_name
        domain_id <- concept_row$domain_id
        
        tryCatch({
          logger::log_info("Processing concept_id: ", concept_id, " (", concept_name, ")")
          result <- code_x_time(
            concept_id = concept_id,
            cdm_schema = cdm_schema,
            domain = domain_id,
            conn = conn,
            postgres_conn = postgres_conn,
            postgres_table_name = "code_x_time",
            cohort_id = cohort_id,
            database_id = database_id,
            schema_name = schema_name
          )
          
          if (!is.null(result)) {
            code_x_time_results[[as.character(concept_id)]] <- list(
              concept_id = concept_id,
              concept_name = concept_name,
              domain_id = domain_id,
              result = result
            )
          }
        }, error = function(e) {
          logger::log_error("Error processing concept_id ", concept_id, ": ", e$message)
        })
      }
    }
    
    results$code_x_time <- code_x_time_results
    logger::log_success("code_x_time analysis completed for ", length(code_x_time_results), " concepts")
  } else {
    logger::log_info("No concepts found for code_x_time analysis")
    results$code_x_time <- list()
  }
  
  logger::log_success("Time dependent analysis completed for cohort_id: ", cohort_id, 
                     " and database_id: ", database_id)
  
  return(results)
}

#' Get time dependent analysis output table names
#'
#' @description Returns the list of table names that are generated by the
#' time dependent analysis functions.
#'
#' @return character vector of table names
#'
#' @author Luisa Martínez (08APR2025)
#' @export
#' @family time_dependent_analysis
get_time_dependent_analysis_output_table_names <- function() {
  tables <- c(
    "patient_eofup",
    "patient_recruitment", 
    "patient_x_time",
    "code_x_time"
  )
  
  return(tables)
}
