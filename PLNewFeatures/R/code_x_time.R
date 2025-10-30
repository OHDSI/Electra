#' Create a table to visualize the number times a concept is reported per time.
#'
#' @description Generate a table to plot the number of times a concept is reported
#' per time window. The function also saves the results to PostgreSQL database.
#'
#' @param concept_id from ATLAS to visualize.
#' @param cdm_schema general database cdm schema.
#' @param domain from the cdm where the code is found options are "Condition",
#' "Drug", "Procedure", "Device", "Measurement" or "Observation"
#' @param conn database connection for querying data
#' @param postgres_conn PostgreSQL connection for saving results (optional)
#' @param postgres_table_name PostgreSQL table name for saving results (optional)
#' @param cohort_id cohort identifier for the analysis (optional)
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
code_x_time <- function(concept_id,
                        cdm_schema,
                        domain,
                        conn,
                        postgres_conn = NULL, postgres_table_name = NULL, cohort_id = NULL, database_id = NULL, 
                        schema_name = "phenotype_library") {

  checkmate::assert_string(cdm_schema, min.chars = 1)

  # Create vectors for the different domains in the omop cdm
  # This could be included in a library
  # Table name
  dom_name <- c("condition_occurrence",
                "drug_exposure",
                "procedure_occurrence",
                "device_exposure",
                "measurement",
                "observation")
  # Concept identificator per table
  dom_concept <- c("condition_concept_id",
                   "drug_concept_id",
                   "procedure_concept_id",
                   "device_concept_id",
                   "measurement_concept_id",
                   "observation_concept_id")
  # Concept start date per table
  dom_start <- c("condition_start_date",
                   "drug_exposure_start_date",
                   "procedure_date",
                   "device_exposure_start_date",
                   "measurement_date",
                   "observation_date")
  # Indication in atlas that correspond to table name
  dom_atlas <- c("Condition",
                 "Drug",
                 "Procedure",
                 "Device",
                 "Measurement",
                 "Observation")

  # Determine Domain from the concept
  dom <- which(dom_atlas == domain)
  qry <- paste0("SELECT ", dom_start[dom],
                " AS start_date, COUNT (*) AS count FROM (SELECT DISTINCT ", dom_start[dom] ,", person_id ",
                "FROM ", cdm_schema,".", dom_name[dom], " WHERE ",
                dom_concept[dom], " = '", concept_id,"') GROUP BY ", dom_start[dom])

  time_dep <- data.table::as.data.table(DBI::dbGetQuery(conn, qry)
  )

  if (nrow(time_dep) > 0) {

    time_dep[, year_month := as.Date(format(start_date, "%Y-%m-01"))]
    to_plot <- time_dep[, .(total_count = sum(count)), by = year_month]

    # Save to PostgreSQL if connection and table name provided
    to_plot <- save_to_postgres(
      postgres_conn = postgres_conn,
      postgres_table_name = postgres_table_name,
      data_frame = to_plot,
      cohort_id = cohort_id,
      database_id = database_id,
      concept_id = concept_id,
      schema_name = schema_name,
      function_name = paste0("code_x_time (concept_id: ", concept_id, ")")
    )

    return(to_plot)

  }

}
