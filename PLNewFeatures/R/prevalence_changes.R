#' Prevalence changes between the cohort and the general population for the most
#' frequent codes
#'
#' @description Create a table with the codes that has at least a prevalence
#' change of 5. The table includes codes from all the domains (Concept id,
#' Concept Name, Domain), the proportion of the code within the whole population,
#' the proportion of the code within the cohort and the prevalence change result.
#' The function also saves the results to PostgreSQL database.
#'
#' @param cohort_id atlas cohort id of the phenotype to be evaluated
#' @param scratch scratch space where the cohort is available
#' @param cdm_schema cdm schema name from the dataset
#' @param concept_sets concepts from the concept set with at least the following
#' columns (Id = concept_id, Name = concept_name, Domain = as retrieved from ATLAS)
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
#' * dplyr
#'
#' ## Required functions
#' * table_freq_concepts
#' * table_freq_spcf_concepts
#'
#' ## Other requirements
#' * The connection to the dataset will have to be created before running the function.
#'
#' @author Luisa Martínez (15SEP2024)
#' @import data.table
#' @import DBI
#' @importFrom dplyr %>%
#' @importFrom rlang %||%
prevalence_changes <- function(cohort_id,
                               scratch,
                               cdm_schema,
                               concept_sets,
                               conn,
                               postgres_conn = NULL,
                               postgres_table_name = NULL,
                               database_id = NULL,
                               schema_name = "phenotype_library") {
  logger::log_info("(prevalence_changes) Function called with schema_name: ", schema_name)
  checkmate::assert_data_frame(concept_sets)
  
  # Handle both column naming conventions (renamed vs original)
  if ("Id" %in% names(concept_sets) && "Domain" %in% names(concept_sets) && "Name" %in% names(concept_sets)) {
    # Data has renamed columns, rename them to expected names
    concept_sets <- concept_sets %>%
      dplyr::rename(concept_id = Id, concept_name = Name, domain_id = Domain)
  } else {
    # Data has original column names, check they exist
    checkmate::assert_names(names(concept_sets), must.include = c("concept_id"))
  }
  checkmate::expect_string(
    scratch,
    pattern = "^[^.]+\\.[^.]+(\\.[^.]+)?$",
    info = "scratch argument requires format: scratch_space_name.table_name"
  )
  stopifnot("conn is missing" = !missing(conn))
  checkmate::assert_class(conn, "DatabaseConnectorJdbcConnection")


  # Create vectors for the different domains in the omop cdm
  # This could be included in a library
  # Table name
  dom_name <- c(
    "condition_occurrence",
    "drug_exposure",
    "procedure_occurrence",
    "device_exposure",
    "measurement",
    "observation"
  )
  # Concept identification per table
  dom_concept <- c(
    "condition_concept_id",
    "drug_concept_id",
    "procedure_concept_id",
    "device_concept_id",
    "measurement_concept_id",
    "observation_concept_id"
  )
  # Indication in atlas that correspond to table name
  dom_atlas <- c(
    "Condition",
    "Drug",
    "Procedure",
    "Device",
    "Measurement",
    "Observation"
  )

  complete_table <- NULL

  # Iterate over all the domains from the omop cdm
  for (n in 1:length(dom_name)) {
    logger::log_info("(prevalence_changes) {n}/{length(dom_name)}: Running for: {dom_name[n]}")


    # Select the most frequent codes with at least 5% of patients from the
    # specific domain
    query <- table_freq_concepts(
      dom_name[n],
      dom_concept[n],
      scratch,
      cdm_schema,
      cohort_id,
      5
    )

    freq_con_cohort <- DBI::dbGetQuery(conn, query)

    # For the most frequent codes in the cohort, calculate the frequency in the
    # general population
    query <- table_freq_spcf_concepts(
      dom_name[n],
      dom_concept[n],
      paste0(freq_con_cohort$concept_id, collapse = ","),
      cdm_schema
    )

    freq_con_total <- DBI::dbGetQuery(conn, query)

    # Merge the frequency in the cohort with the frequency in the general population
    freq_table <- data.table::as.data.table(merge(freq_con_cohort,
      freq_con_total,
      by = "concept_id"
    ))

    # Include the domain for the final table
    freq_table[, domain := dom_atlas[n]]
    # Calculate the prevalence change and keep PC higher than 5
    freq_table[, comparator := pop_perc / total_pop_perc]
    freq_table <- freq_table[comparator > 5]

    # Join tables by domain
    complete_table <- rbind(
      complete_table,
      freq_table
    )
    logger::log_info("(prevalence_changes) Finished for: {dom_name[n]}")

  }

  # save(complete_table, concept_sets, file = "dev/prev_changes.RDa")
  logger::log_info("(prevalence_changes) Merging tables")
  # Clean the table to have specific format to be shown
  table_to_print <- complete_table[
    !(concept_id %in% concept_sets$concept_id),
    .(
      "Concept id" = concept_id,
      "Concept Name" = concept_name,
      "Domain" = domain,
      "Proportion within the cohort (Pc)" = pop_perc,
      "Proportion within the total population (Pt)" = total_pop_perc,
      "Pc/Pt" = comparator
    )
  ] %>%
    dplyr::mutate(dplyr::across(dplyr::all_of(
      c(
        "Proportion within the cohort (Pc)",
        "Proportion within the total population (Pt)",
        "Pc/Pt"
      )
    ), ~ round(.x, digits = 2)))

  # Save to PostgreSQL if connection and table name provided
  if (!is.null(postgres_conn) && !is.null(postgres_table_name) && !is.null(database_id)) {
    # Prepare data for PostgreSQL storage
    postgres_data <- table_to_print %>%
      dplyr::mutate(
        cohort_id = cohort_id,
        database_id = database_id,
        # Rename columns to match PostgreSQL naming conventions
        concept_id = `Concept id`,
        concept_name = `Concept Name`,
        domain = Domain,
        proportion_cohort = `Proportion within the cohort (Pc)`,
        proportion_total_population = `Proportion within the total population (Pt)`,
        pc_pt_ratio = `Pc/Pt`
      ) %>%
      dplyr::select(cohort_id, database_id, concept_id, concept_name, domain, 
                   proportion_cohort, proportion_total_population, pc_pt_ratio)
    
    # Save to PostgreSQL using helper function
    logger::log_info("(prevalence_changes) About to save to PostgreSQL with schema_name: ", schema_name)
    postgres_data <- save_to_postgres(
      postgres_conn = postgres_conn,
      postgres_table_name = postgres_table_name,
      data_frame = postgres_data,
      cohort_id = cohort_id,
      database_id = database_id,
      schema_name = schema_name,
      function_name = "prevalence_changes"
    )
  }

  logger::log_success("(prevalence_changes) Finished")
  return(table_to_print)
}
