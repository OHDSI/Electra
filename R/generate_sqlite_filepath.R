#' generate_sqlite_filepath
#'
#' @param scratch_space_name
#' @param username
#' @param password
#' @param atlas_user_name
#' @param atlas_password
#' @param CohortID
#' @param server
#' @param multiple_databaseCodes
#' @param selectedDatabase
#'
#' @return
#' @export
#' @import CohortDiagnostics
#'
generate_sqlite_filepath <- function(scratch_space_name,
                                     username,
                                     password,
                                     atlas_user_name,
                                     atlas_password,
                                     CohortID,
                                     server,
                                     multiple_databaseCodes,
                                     selectedDatabase,
                                     export_dir = file.path(getwd(), "export"),
                                     drop_cohort_stats_tables = FALSE,
                                     drivers_dir = paste(getwd(), "drivers", sep = "/"),
                                     send_notification_email = FALSE,
                                     session_user = NULL,
                                     schema_name = "phenotype_library") {

  if (send_notification_email && !shiny::isTruthy(session_user)) {
    stop("send_notification_email is set to TRUE, so valid session_user must be provided.")
  }

  if (interactive()) {
    while (sink.number() > 0) sink()
  }

  # Convert cohort_id to numeric if necessary
  CohortID <- gsub("__COMMA__", ",", CohortID)

  cohort_id_split <- as.double(unlist(strsplit(as.character(CohortID), "_")))

  cdmDatabaseSchema <- get_cdm_schema(selectedDatabase)

  logger::log_info("cdmDatabaseSchema is: ", toString(cdmDatabaseSchema, Inf))

  cohortTable <- get_cohort_table_for_evaluation()

  connectionDetails <- get_databricks_conn_details(drivers_dir = "drivers")

  tempEmulationSchema <- scratch_space_name

  baseUrl <- Sys.getenv("Atlas_url")
  httr::set_config(httr::config(ssl_verifypeer = 0L))
  ROhdsiWebApi::authorizeWebApi(
    baseUrl = baseUrl,
    authMethod = "windows",
    webApiUsername = atlas_user_name,
    webApiPassword = atlas_password
  )

  cohortDefinitionSet <- ROhdsiWebApi::exportCohortDefinitionSet(
      baseUrl = baseUrl,
      cohortIds = as.double(cohort_id_split),
      generateStats = TRUE
    )

  cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = cohortTable)

  logger::log_info("Testing connection to: ", scratch_space_name)
  tryCatch({
    DatabaseConnector::connect(connectionDetails)
  }, error = function(e) {
    msg <- glue::glue("Failed to connect to '{scratch_space_name}' as user '{username}' (serer: {server}). Error: {e$message}")
    logger::log_error(msg)
    stop(msg)
  })


  # Create the tables schema within the scratch space of the database with containing cdm data
  CohortGenerator::createCohortTables(
    connectionDetails = connectionDetails,
    cohortTableNames = cohortTableNames,
    cohortDatabaseSchema = scratch_space_name,
    incremental = FALSE
  )
  # Generate the cohort defined in ATLAS within the scratch space of the database containing cdm data

  # save.image("dev/pl-session-generateCohortSet.RData")
  CohortGenerator::generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = scratch_space_name,
    tempEmulationSchema = tempEmulationSchema,
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortDefinitionSet,
    incremental = FALSE
  )

  exportFolder <- paste(export_dir, "merged", multiple_databaseCodes, sep = "/")
  sqliteDbPath <- file.path(exportFolder, glue::glue("{paste0(cohort_id_split, collapse = \"_\")}.sqlite"))
  if (file.exists(sqliteDbPath)) {
    unlink(sqliteDbPath, TRUE, TRUE)
  }

  if (F) {
    #  to be tested
    BinaryCache::with_cache(
      output_directory = exportFolder,
      processing_function = CohortDiagnostics::executeDiagnostics,
      processing_args = list(
        cohortDefinitionSet = cohortDefinitionSet,
        connectionDetails = connectionDetails,
        cohortTable = cohortTable,
        cohortDatabaseSchema = scratch_space_name,
        cdmDatabaseSchema = cdmDatabaseSchema,
        exportFolder = exportFolder,
        databaseId = selectedDatabase,
        minCellCount = 5
      ),
      cache_method = "cachem",
      custom_cachem = get_cache_obj(),
      cache_params = c("cdmDatabaseSchema", "cohortTable", "cdmDatabaseSchema", "cohortDefinitionSet")
    )
  }

  CohortDiagnostics::executeDiagnostics(
    cohortDefinitionSet = cohortDefinitionSet,
    connectionDetails = connectionDetails,
    cohortTable = cohortTable,
    cohortDatabaseSchema = scratch_space_name,
    tempEmulationSchema = tempEmulationSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    exportFolder = exportFolder,
    databaseId = selectedDatabase,
    minCellCount = 5
  )

  # Drop the tables from the scratch space of the database containing cdm data
  if (drop_cohort_stats_tables) {
    CohortGenerator::dropCohortStatsTables(
      connectionDetails = connectionDetails,
      cohortDatabaseSchema = scratch_space_name,
      cohortTableNames = cohortTableNames
    )
  }

  logger::log_info("Export folder: ", exportFolder)
  logger::log_info("sqliteDbPath: ", sqliteDbPath)

  tryCatch(
    {
      logger::log_info(glue::glue("(generate_sqlite_filepath) Merging into sqlite: {sqliteDbPath}"))

      merge_output <- CohortDiagnostics::createMergedResultsFile(
        exportFolder,
        sqliteDbPath = sqliteDbPath,
        overwrite = TRUE # do not set to TRUE because correlation analysis tables will be lost
      )
    },
    error = function(e) {
      logger::log_error("An error occurred in the Create Merged Results Function: ", e$message)
    }
  )

  logger::log_info("Reading concept sets")
  checkmate::assert_file_exists(sqliteDbPath)

  included_source_concept.csv <-  file.path(exportFolder, "included_source_concept.csv")

  if (!file.exists(included_source_concept.csv)) {
    stop(glue::glue("CohortDiagnostics::executeDiagnostics did not generate {included_source_concept.csv} for "),
         glue::glue("database {selectedDatabase} and Cohort ID(s) {toString(CohortID, Inf)}"))
  }

  included_source_concept.csv <-  file.path(exportFolder, "included_source_concept.csv")

  if (!file.exists(included_source_concept.csv)) {
    stop(glue::glue("CohortDiagnostics::executeDiagnostics did not generate {included_source_concept.csv} for "),
         glue::glue("database {selectedDatabase} and Cohort ID(s) {toString(CohortID, Inf)}"))
  }

  concept_sets_df <- get_concept_sets(
    included_source_concept.csv = included_source_concept.csv,
    concept.csv = file.path(exportFolder, "concept.csv"),
    rename_cols = FALSE
  )

  conn_scratch <- DatabaseConnector::connect(connectionDetails)
  on.exit(DBI::dbDisconnect(conn_scratch), add = TRUE)
  conn_sqlite <- DBI::dbConnect(RSQLite::SQLite(), sqliteDbPath)
  on.exit(DBI::dbDisconnect(conn_sqlite), add = TRUE)

  m_correlation_analysis <- memoise::memoise(correlation_analysis,
                                             cache = get_cache_obj(),
                                             omit_args = c("conn", "max_cores"))

  # Memoize time dependent analysis function
  m_time_dependent_analysis <- memoise::memoise(time_dependent_analysis,
                                                cache = get_cache_obj(),
                                                omit_args = c("conn", "postgres_conn"))

  # Memoize prevalence changes analysis function
  m_prevalence_changes_analysis <- memoise::memoise(prevalence_changes_analysis,
                                                   cache = get_cache_obj(),
                                                   omit_args = c("conn", "postgres_conn"))

  # save.image("dev/pl-session.RData")

  list_tables_per_cohort_id <- list()



  for (coh_id in cohort_id_split) {
    logger::log_info("Running correlation_analysis for Cohort ID: ", coh_id)
    list_tables <- m_correlation_analysis(
      cohort_id = coh_id,
      scratch = glue::glue("{scratch_space_name}.{cohortTable}"),
      cdm_schema = cdmDatabaseSchema,
      concept_sets = concept_sets_df,
      conn = conn_scratch,
      server = server,
      drivers_dir = drivers_dir,
      max_cores = future::availableCores() %/% 10L
    )

    list_tables2 <- list_tables %>%
      names() %>%
      lapply(function(nm) {
        df <- list_tables[[nm]]
        # add mandatory tables for PostgreSQL
        df$database_id <- selectedDatabase
        df$cohort_id <- coh_id
        df
      })
    names(list_tables2) <- names(list_tables)

    to_append <- list(list_tables2)
    names(to_append) <- coh_id
    list_tables_per_cohort_id <- append(list_tables_per_cohort_id, to_append)
  }

  # Run time dependent analysis for each cohort
  logger::log_info("Starting time dependent analysis for all cohorts...")
  postgres_conn <- DatabaseConnector::connect(get_postgresql_connection_details())
  on.exit(DBI::dbDisconnect(postgres_conn), add = TRUE)

  time_dep_tables_per_cohort_id <- list()

  for (coh_id in cohort_id_split) {
    logger::log_info("Running time_dependent_analysis for Cohort ID: ", coh_id)
    tryCatch({
      time_dep_results <- m_time_dependent_analysis(
        cohort_id = coh_id,
        scratch.table = glue::glue("{scratch_space_name}.{cohortTable}"),
        cdm_schema = cdmDatabaseSchema,
        concept_sets = concept_sets_df,
        conn = conn_scratch,
        postgres_conn = postgres_conn,
        database_id = selectedDatabase,
        server = server,
        drivers_dir = drivers_dir,
        schema_name = schema_name
      )

      # Process time dependent analysis results for SQLite storage
      if (!is.null(time_dep_results)) {
        time_dep_tables2 <- list()

        # Process patient_eofup
        if (!is.null(time_dep_results$patient_eofup)) {
          df <- time_dep_results$patient_eofup
          df$database_id <- selectedDatabase
          df$cohort_id <- coh_id
          time_dep_tables2$patient_eofup <- df
        }

        # Process patient_recruitment
        if (!is.null(time_dep_results$patient_recruitment)) {
          df <- time_dep_results$patient_recruitment
          df$database_id <- selectedDatabase
          df$cohort_id <- coh_id
          time_dep_tables2$patient_recruitment <- df
        }

        # Process patient_x_time
        if (!is.null(time_dep_results$patient_x_time)) {
          df <- time_dep_results$patient_x_time
          df$database_id <- selectedDatabase
          df$cohort_id <- coh_id
          time_dep_tables2$patient_x_time <- df
        }

        # Process code_x_time (this is a list of results)
        checkmate::assert_list(time_dep_results$code_x_time)
        if (!is.null(time_dep_results$code_x_time) && length(time_dep_results$code_x_time) > 0) {
          code_x_time_list <- list()
          for (concept_result in time_dep_results$code_x_time) {
            if (!is.null(concept_result$result)) {
              df <- concept_result$result
              df$database_id <- selectedDatabase
              df$cohort_id <- coh_id
              df$concept_id <- concept_result$concept_id
              code_x_time_list[[as.character(concept_result$concept_id)]] <- df
            }
          }
          if (length(code_x_time_list) > 0) {
            time_dep_tables2$code_x_time <- dplyr::bind_rows(code_x_time_list)
          }
        }

        to_append <- list(time_dep_tables2)
        names(to_append) <- coh_id
        time_dep_tables_per_cohort_id <- append(time_dep_tables_per_cohort_id, to_append)
      }

      logger::log_success("(generate_sqlite_filepath) Time dependent analysis completed for cohort_id: ", coh_id)
    }, error = function(e) {
      logger::log_error("(generate_sqlite_filepath) Error in time dependent analysis for cohort_id ", coh_id, ": ", e$message)
    })
  }

  # Run prevalence changes analysis for each cohort
  logger::log_info("(generate_sqlite_filepath) Starting prevalence changes analysis for all cohorts...")

  prevalence_tables_per_cohort_id <- list()

  for (coh_id in cohort_id_split) {
    logger::log_info("(generate_sqlite_filepath) Running prevalence_changes_analysis for Cohort ID: ", coh_id)
    tryCatch({
      prevalence_results <- m_prevalence_changes_analysis(
        cohort_id = coh_id,
        scratch.table = glue::glue("{scratch_space_name}.{cohortTable}"),
        cdm_schema = cdmDatabaseSchema,
        concept_sets = concept_sets_df,
        conn = conn_scratch,
        postgres_conn = postgres_conn,
        database_id = selectedDatabase,
        server = server,
        drivers_dir = drivers_dir,
        schema_name = schema_name
      )

      # Process prevalence changes results for SQLite storage
      if (!is.null(prevalence_results)) {
        prevalence_tables2 <- list()

        # Process prevalence_changes
        df <- prevalence_results
        df$database_id <- selectedDatabase
        df$cohort_id <- coh_id
        # Rename columns to match PostgreSQL format
        df <- df %>%
          dplyr::mutate(
            concept_id = `Concept id`,
            concept_name = `Concept Name`,
            domain = Domain,
            proportion_cohort = `Proportion within the cohort (Pc)`,
            proportion_total_population = `Proportion within the total population (Pt)`,
            pc_pt_ratio = `Pc/Pt`
          ) %>%
          dplyr::select(cohort_id, database_id, concept_id, concept_name, domain,
                       proportion_cohort, proportion_total_population, pc_pt_ratio)

        prevalence_tables2$prevalence_changes <- df

        to_append <- list(prevalence_tables2)
        names(to_append) <- coh_id
        prevalence_tables_per_cohort_id <- append(prevalence_tables_per_cohort_id, to_append)
      }

      logger::log_success("(generate_sqlite_filepath) Prevalence changes analysis completed for cohort_id: ", coh_id)
    }, error = function(e) {
      logger::log_error("(generate_sqlite_filepath) Error in prevalence changes analysis for cohort_id ", coh_id, ": ", e$message)
    })
  }

  all_names <- unique(unlist(lapply(list_tables_per_cohort_id, names)))

  corr_tables <- setNames(
    lapply(all_names, function(nm) {
      dfs <- lapply(list_tables_per_cohort_id, function(sublist) sublist[[nm]])
      dfs <- Filter(Negate(is.null), dfs)
      dplyr::bind_rows(dfs)
    }),
    all_names
  )

  # Process time dependent analysis tables
  if (length(time_dep_tables_per_cohort_id) > 0) {
    time_dep_all_names <- unique(unlist(lapply(time_dep_tables_per_cohort_id, names)))

    time_dep_tables <- setNames(
      lapply(time_dep_all_names, function(nm) {
        dfs <- lapply(time_dep_tables_per_cohort_id, function(sublist) sublist[[nm]])
        dfs <- Filter(Negate(is.null), dfs)
        dplyr::bind_rows(dfs)
      }),
      time_dep_all_names
    )
  } else {
    time_dep_tables <- list()
  }

  # Process prevalence changes tables
  if (length(prevalence_tables_per_cohort_id) > 0) {
    prevalence_all_names <- unique(unlist(lapply(prevalence_tables_per_cohort_id, names)))

    prevalence_tables <- setNames(
      lapply(prevalence_all_names, function(nm) {
        dfs <- lapply(prevalence_tables_per_cohort_id, function(sublist) sublist[[nm]])
        dfs <- Filter(Negate(is.null), dfs)
        dplyr::bind_rows(dfs)
      }),
      prevalence_all_names
    )
  } else {
    prevalence_tables <- list()
  }

  saveRDS(corr_tables, file = file.path(exportFolder, glue::glue("list_tables.RDS")))
  saveRDS(time_dep_tables, file = file.path(exportFolder, glue::glue("time_dep_tables.RDS")))
  saveRDS(prevalence_tables, file = file.path(exportFolder, glue::glue("prevalence_tables.RDS")))
  # save(exportFolder, list_tables, cohort_id_split, coh_id, cdmDatabaseSchema, cohortDatabaseSchema, concept_sets_df, merge_output,
  #      file = "~/projects/pl_odysseus2/dev/generate_sqlite_filepath-vars-2.RData")


  logger::log_info("(generate_sqlite_filepath) Appending correlation analysis tables to SQLite...")
  list_tables_paths <- corr_tables %>%
    names() %>%
    sapply(function(table_name) {
      table_name_in_db <- correlation_table_name(table_name, "", "")
      fileName <- file.path(exportFolder, glue::glue("{table_name_in_db}.csv"))
      table_df <- corr_tables[[table_name]]

      logger::log_info(glue::glue("(generate_sqlite_filepath) Writing table '{table_name}' to '{fileName}'"))
      readr::write_csv(x = table_df, file = fileName)

      logger::log_info(glue::glue("(generate_sqlite_filepath) Writing table '{table_name_in_db}' to DB '{sqliteDbPath}'"))
      DBI::dbWriteTable(conn_sqlite, table_name_in_db, table_df, overwrite = TRUE)

      fs::path_rel(fileName, start = exportFolder)
    }, USE.NAMES = FALSE)

  # Save time dependent analysis tables to CSV and append to SQLite
  if (length(time_dep_tables) > 0) {
    logger::log_info("(generate_sqlite_filepath) Appending time dependent analysis tables to SQLite...")
    time_dep_tables_paths <- time_dep_tables %>%
      names() %>%
      sapply(function(table_name) {
        fileName <- file.path(exportFolder, glue::glue("{table_name}.csv"))
        table_df <- time_dep_tables[[table_name]]

        logger::log_info(glue::glue("(generate_sqlite_filepath) Writing time dependent table '{table_name}' to '{fileName}'"))
        readr::write_csv(x = table_df, file = fileName)

        logger::log_info(glue::glue("(generate_sqlite_filepath) Writing time dependent table '{table_name}' to DB '{sqliteDbPath}'"))
        DBI::dbWriteTable(conn_sqlite, table_name, table_df, overwrite = TRUE)

        fs::path_rel(fileName, start = exportFolder)
      }, USE.NAMES = FALSE)
  } else {
    time_dep_tables_paths <- character(0)
  }

  # Save prevalence changes tables to CSV and append to SQLite
  if (length(prevalence_tables) > 0) {
    logger::log_info("(generate_sqlite_filepath) Appending prevalence changes tables to SQLite...")
    prevalence_tables_paths <- prevalence_tables %>%
      names() %>%
      sapply(function(table_name) {
        fileName <- file.path(exportFolder, glue::glue("{table_name}.csv"))
        table_df <- prevalence_tables[[table_name]]

        logger::log_info(glue::glue("(generate_sqlite_filepath) Writing prevalence changes table '{table_name}' to '{fileName}'"))
        readr::write_csv(x = table_df, file = fileName)

        logger::log_info(glue::glue("(generate_sqlite_filepath) Writing prevalence changes table '{table_name}' to DB '{sqliteDbPath}'"))
        DBI::dbWriteTable(conn_sqlite, table_name, table_df, overwrite = TRUE)

        fs::path_rel(fileName, start = exportFolder)
      }, USE.NAMES = FALSE)
  } else {
    prevalence_tables_paths <- character(0)
  }

  uploaded_s3_object <- upload_sqlite_to_s3(sqliteDbPath, s3_bucket_name = Sys.getenv("S3_bucket_server"), s3_key_prefix = "CD_SQLITE/OPTUM_EHR")

  if (send_notification_email) {
    send_mail_sqlite_to_s3(uploaded_s3_object =  uploaded_s3_object, CohortID = CohortID, databases = selectedDatabase, session_user = session_user)
  }

  # TODO: should include multiple tables per cohort
  return(list(
    list_tables = corr_tables,
    list_tables_paths = list_tables_paths,
    time_dep_tables = time_dep_tables,
    time_dep_tables_paths = time_dep_tables_paths,
    prevalence_tables = prevalence_tables,
    prevalence_tables_paths = prevalence_tables_paths,
    exportFolder = exportFolder,
    sqliteDbPath = sqliteDbPath
  ))
  # Returns the path where the generated sql file is stored
} # End of generate_sqlite_filepath function
