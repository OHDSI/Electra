#' Correlation analysis for code recommendation when evaluating a phenotype
#'
#' @description Create a list of tables with the result of the quantitative correlation
#'  between the appearance of the codes from the concept set and the 30 most
#'  frequent codes per domain between the 6 domains from the omop cdm (Condition,
#'  Drug, Procedure, Device, Measurement, Observation).
#'
#' @param cohort_id atlas cohort id of the phenotype to be evaluated
#' @param scratch scratch space where the cohort is available
#' @param cdm_schema cdm schema name from the dataset
#' @param concept_sets concepts from the concept set with at least the following
#' columns (Id = concept_id, Name = concept_name, Domain = as retrieved from ATLAS)
#'
#' @details
#'
#' ## Required packages
#' * data.table
#'
#' ## Required functions
#' * table_freq_concepts
#' * query_patientxconcept
#'
#' ## Other requirements
#' * The connection to the dataset will have to be created before running the function.
#'
#' @author Luisa Martínez (10SEP2024)
correlation_analysis <- function(cohort_id,
                                 scratch,
                                 cdm_schema,
                                 concept_sets,
                                 conn,
                                 server,
                                 drivers_dir,
                                 max_cores = future::availableCores() %/% 3L,
                                 filter_domains_to = c("condition_occurrence"),
                                 parallel_run = FALSE) {
  checkmate::assert_data_frame(concept_sets)
  stopifnot("conn is missing" = !missing(conn))
  checkmate::assert_class(conn, "DatabaseConnectorJdbcConnection")

  if (all(!c("Id", "Domain", "Name") %in% names(concept_sets)) && all(c("concept_id", "domain_id", "concept_name") %in% names(concept_sets))) {
    logger::log_info("Renaming concept sets columns")
    concept_sets <- concept_sets %>%
      dplyr::select(Id = concept_id, Domain = domain_id, Name = concept_name)

  }

  checkmate::assert_names(names(concept_sets), must.include = c("Id", "Domain", "Name"))
  checkmate::expect_string(
    scratch,
    pattern = "^[^.]+\\.[^.]+(\\.[^.]+)?$",
    info = "scratch argument requires format: scratch_space_name.table_name"
  )
  checkmate::assert_string(server, min.chars = 1L, na.ok = TRUE)
  checkmate::assert_string(drivers_dir, min.chars = 1L)
  checkmate::assert_numeric(max_cores)


  m_dbGetQuery <- memoise::memoise(DBI::dbGetQuery, cache = get_cache_obj(), omit_args = "conn")

  # Set up parallel processing
  # Use one less than available cores to avoid overloading your system
  # n_cores <- if (future::availableCores() > max_cores) { max_cores } else {future::availableCores() - 1}
  # logger::log_info(glue::glue("Using {n_cores} cores"))
  # future::plan(future::multisession, workers = max_cores)

  if (parallel_run) {
    logger::log_info(glue::glue("Setting future plan using {max_cores} cores"))
    future::plan(future::multisession, workers = max_cores)
  }

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

  # Concept identificator per table
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

  if (checkmate::test_choice(filter_domains_to, choices = dom_name)) {
    checkmate::assert_choice(filter_domains_to, choices = dom_name)
    logger::log_info("(correlation_analysis) Filtering domains to: {toString(filter_domains_to, Inf)}")
    idx <- which(dom_name == filter_domains_to)
    dom_name <- dom_name[dom_name %in% filter_domains_to]
    dom_concept <- dom_concept[idx]
    dom_atlas <- dom_atlas[idx]
  }

  logger::log_info("(correlation_analysis} dom_name: ", toString(dom_name, Inf))
  logger::log_info("(correlation_analysis} dom_concept: ", toString(dom_concept, Inf))
  logger::log_info("(correlation_analysis} dom_atlas: ", toString(dom_atlas, Inf))

  # Create an empty list to store the tables
  output_tables <- list()
  # 6 tables will be generated, one per domain. Iterate over the domains to create them.
  for (n in 1:length(dom_name)) {
    tryCatch(
      {
        logger::log_info("(correlation_analysis) Running for: {dom_name[n]}")
        # Initialize the complete correlations table
        comp_cor <- NULL
        # Select the 30 most frequent codes with at least 30% of patients from the
        # specific domain
        query <- paste0(
          table_freq_concepts(
            dom_name[n],
            dom_concept[n],
            scratch,
            cdm_schema,
            cohort_id,
            1
          ), " LIMIT 30")

        freq_concpt <- data.table::as.data.table(DBI::dbGetQuery(conn, query))
        # freq_concpt <- freq_concpt[!(concept_id %in% concept_sets$Id)] # TODO: remove
        freq_concpt <- freq_concpt %>%
          dplyr::filter(!concept_id %in% concept_sets$Id) %>%
          data.table::as.data.table()

        # Store the most frequent concepts in the list for future use
        output_tables[[paste0(dom_name[n], "_freq_concepts")]] <- freq_concpt
        # Iterate over the concepts from the concept_set
        for (c in 1:length(concept_sets$Id)) {
          logger::log_info("(correlation_analysis) Running for Concept Set #{c}/{length(concept_sets$Id)}: {concept_sets$Name[c]}")

          # Determine Domain from the concept
          dom <- which(dom_atlas == concept_sets$Domain[c])
          # Generate a query to extract the patients id containing the concept from omop cdm

          domain_table <- dom_name[dom]
          if (!checkmate::test_string(domain_table, min.chars = 1)) {
            logger::log_warn("domain_table not found. Probably because of filtered dom_name: ",
                             toString(dom_name, Inf),
                             ". Skipping iteration...")
            next
          }

          query_vector <- query_patientxconcept(
            domain_table = domain_table,
            domain_concept_id = dom_concept[dom],
            scratch = scratch,
            cdm_schema = cdm_schema,
            concept_id = concept_sets$Id[c],
            cohort_id = cohort_id
          )

          new_vector <- dbGetQuery(conn, query_vector)

          # If there is at least one patient with the code, include it in the final table
          # where there is a row per patient and a binary column per code and the
          # first code is the one to be compare with the others
          if (nrow(new_vector) > 0) {
            # Create binary column
            new_vector$concept_id <- 1
            colnames(new_vector) <- c("person_id", concept_sets$Name[c])
            patient <- new_vector

            # Iterate over the most frequent concepts to complete the correlations tabl

            if (F) {
              for (code in 1:nrow(freq_concpt)) {

                if (!"code" %in% names(freq_concpt)) {
                  logger::log_warn(glue::glue("Column 'code' in missing from freq_concpt data frame. Skipping interation..."))
                  next
                }

                if (!"concept_id" %in% names(freq_concpt)) {
                  logger::log_warn(glue::glue("Column 'concept_id' in missing from freq_concpt data frame. Skipping interation..."))
                  next
                }

                concept_id <- freq_concpt %>%
                  dplyr::slice(code) %>%
                  dplyr::pull(concept_id)
                logger::log_info(glue::glue("concept_id {concept_id} #{code}/{nrow(freq_concpt)}"))

                # Generate a query to extract the patients id containing the concept from omop cdm
                query_vector <- query_patientxconcept(
                  dom_name[n],
                  dom_concept[n],
                  scratch,
                  cdm_schema,
                  # freq_concpt[code, concept_id],
                  concept_id,
                  cohort_id
                )

                new_vector <- dbGetQuery(conn, query_vector)
                # Create binary column
                new_vector$concept_id <- 1
                # colnames(new_vector) <- c("person_id", freq_concpt[code, concept_name])
                colnames(new_vector) <- c(
                  "person_id",
                  freq_concpt %>% dplyr::slice(code) %>% dplyr::pull(concept_name)
                )

                # Marge each freq code with the "target" code of interest (from the concept set)
                # to create the table for the correlation analysis
                patient <- merge(patient,
                  new_vector,
                  all.x = TRUE,
                  all.y = TRUE,
                  by = "person_id"
                )
              }
            }



            # -----

            creds_yaml <- get_credentials_path()
            logger::log_info("Credentials locations: {creds_yaml}")

            logger::log_info("Starting loop...")

            # Replace the for loop with future_map
            # new_vectors <- furrr::future_map(1:nrow(freq_concpt), function(code) {
            new_vectors <- purrr::map(1:nrow(freq_concpt), function(code) {
              # devtools::load_all()
              # devtools::load_all("PLNewFeatures")
              # set_credentials(verbose = FALSE, path = creds_yaml)
              # conn <- get_redshift_conn(server, drivers_dir)
              # on.exit(DBI::dbDisconnect(conn))

              checkmate::assert_names(names(freq_concpt), must.include = "concept_id")

              # Extract the concept_id using dplyr operations
              c_id <- freq_concpt %>%
                dplyr::slice(code) %>%
                dplyr::pull(concept_id)

              # Log information
              logger::log_info("concept_id '{c_id}'\t#{code}/{nrow(freq_concpt)}")

              # Generate a query to extract the patients' id containing the concept
              query_vector <- query_patientxconcept(
                dom_name[n],
                dom_concept[n],
                scratch,
                cdm_schema,
                # freq_concpt[code, concept_id],
                c_id,
                cohort_id
              )

              # Execute the query
              new_vector <- dbGetQuery(conn, query_vector)

              if (nrow(new_vector) == 0L) {
                return(data.frame())
              }

              # Create binary column
              new_vector$concept_id <- 1

              # Set column names
              colnames(new_vector) <- c(
                "person_id",
                freq_concpt %>% dplyr::slice(code) %>% dplyr::pull(concept_name)
              )

              return(new_vector)
            })
            # },
            # .options = furrr::furrr_options(
            #   globals = c(
            #     "freq_concpt",
            #     "dom_name",
            #     "dom_concept",
            #     "scratch",
            #     "cdm_schema",
            #     "cohort_id",
            #     "n",
            #     "m_dbGetQuery",
            #     "server",
            #     "drivers_dir",
            #     "creds_yaml"
            #   )
            # )
            # )

            logger::log_success("Finished loop")


            logger::log_info("Merging data frames...")
            # Convert base data frames to data.table objects
            data.table::setDT(patient)
            new_vectors <- lapply(new_vectors, data.table::setDT)

            # Set keys for optimized merging
            data.table::setkey(patient, person_id)
            new_vectors <- lapply(new_vectors, function(x) data.table::setkey(x, person_id))

            # save(new_vectors, patient, file = "dev/new_vectors.RDa")
            patient <- Reduce(function(x, y) {
              data.table::merge.data.table(x,
                y,
                all.x = TRUE,
                all.y = TRUE,
                by = "person_id"
              )
            }, new_vectors, init = patient)

            # Transform NA into 0 to complete the binary columns
            patient[is.na(patient)] <- 0
            # Analyze the correlations between the codes (excluding patient id column)
            cset_corr <- cor(patient[, -1])

            corr_table <- as.data.frame(cbind(
              rownames(as.data.frame(cset_corr[1, ]))[1],
              rownames(as.data.frame(cset_corr[1, -1])),
              cset_corr[1, -1]
            ))

            rownames(corr_table) <- NULL
            corr_table$V3 <- as.numeric(corr_table$V3)

            comp_cor <- rbind(
              comp_cor,
              corr_table
            )
          }
        }

        output_tables[[paste0(dom_name[n], "_correlations")]] <- comp_cor[]
      },
      error = function(e) {
        logger::log_warn("Failed iteration for Concept Set. Error: ", e$messages)
        concept_set <- concept_sets$Name[c]
        warning(glue::glue("Failed iteration for Concept Set '{toString(concept_set, Inf)}'. Error message: ", e$message))
      }
    )

    logger::log_success("(correlation_analysis) {concept_sets$Name[c]} DONE")
  }

  logger::log_success("(correlation_analysis) FINISHED")
  return(output_tables)
}


#' correlation_table_name
#'
#' @param table_name
#' @param coh_id
#'
#' @returns
#' @export
#'
correlation_table_name <- function(table_name, coh_id, sep = "_") {
  stopifnot(
    "table_name is missing" = !missing(table_name),
    "coh_id is missing" = !missing(coh_id)
  )
  # glue::glue("{table_name}_{coh_id}")
  paste0(table_name, sep, coh_id)
}

#' get_correlation_analysis_output_table_names
#'
#' @returns
#' @export
#'
get_correlation_analysis_output_table_names <- function(cohort_id = NA) {

  tables <- c(
    "condition_occurrence_freq_concepts",
    "condition_occurrence_correlations",
    "drug_exposure_freq_concepts",
    "drug_exposure_correlations",
    "procedure_occurrence_freq_concepts",
    "procedure_occurrence_correlations",
    "device_exposure_freq_concepts",
    "device_exposure_correlations",
    "measurement_freq_concepts",
    "measurement_correlations",
    "observation_freq_concepts",
    "observation_correlations"
  )

  correlation_table_name(tables, "", "") # "" because in 1st implementation table name had suffix "_{cohort ID}"
}
