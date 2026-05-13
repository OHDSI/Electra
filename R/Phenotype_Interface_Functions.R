

#' sqlite_path
#'
#' This function returns arguments that are required to run the server part of Cohort Diagnostics App
#'
#' @param scratch_space_name
#' @param username
#' @param password
#' @param atlas_user_name
#' @param atlas_password
#' @param CohortID
#' @param server
#' @param selectedDatabase
#'
#' @return
#' @export
#'
#' @import DatabaseConnector
#'
sqlite_path <- function(scratch_space_name = NULL,
                        username = NULL,
                        password = NULL,
                        atlas_user_name = NULL,
                        atlas_password = NULL,
                        CohortID = NULL,
                        server = NULL,
                        selectedDatabase = NULL,
                        unlink_outputs = TRUE,
                        export_dir = file.path(getwd(), "export"),
                        force_evaluation = FALSE) {
  # Set AWS credentials from environment variables
  if (as.character.default(Sys.getenv("run_Environment") == "development")) {
    loggit("INFO", "Get AWS Keys and IDs", app = "PhenoType Interface Functions")
  }

  Sys.setenv(
    "AWS_ACCESS_KEY_ID" = Sys.getenv("AWS_ACCESS_KEY_ID"),
    "AWS_SECRET_ACCESS_KEY" = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
    "AWS_DEFAULT_REGION" = Sys.getenv("AWS_DEFAULT_REGION")
  )

  # Specify S3 bucket details
  if (as.character.default(Sys.getenv("run_Environment") == "development")) {
    loggit("INFO", "### Set Bucket details ###", app = "PhenoType Interface Functions")
  }

  # Update the region

  if (as.character.default(Sys.getenv("run_Environment") == "development")) {
    loggit("INFO", "CohortID", app = "PhenoType Interface Functions")
    loggit("INFO", as.character.default(CohortID), app = "PhenoType Interface Functions")

    loggit("INFO", "CohortID[1]", app = " PhenoType Interface Functions")
    loggit("INFO", paste0(CohortID[1]), app = " PhenoType Interface Functions")

    loggit("INFO", "CohortID[2]", app = " PhenoType Interface Functions")
    loggit("INFO", paste0(CohortID[2]), app = " PhenoType Interface Functions")
  }

  # sort the Cohorts when multiple to avoid creating mutiple files for the same cohorts regardless of the order inputed
  CohortID <- sort.default(CohortID)
  CohortID = paste(sort(CohortID), collapse = "_")

  # Checking if the sqlite file for a given cohort id is present
  # The sqlite file is generated only if it is not present in S3 bucket
  if (as.character.default(Sys.getenv("run_Environment") == "development")) {
    loggit("INFO", "BEFORE Preprocessed CohortID", app = "PhenoType Interface Functions")
    loggit("INFO", paste0(CohortID), app = "PhenoType Interface Functions")
  }

  # Preprocess CohortID if multiple IDs were present sort and format
  # CohortID = paste0(sort(strsplit(gsub(',','_',gsub(' ','',as.character(CohortID))),'_')[[1]]), collapse='_')
  CohortID <- paste(sort(CohortID), collapse = "_")

  if (as.character.default(Sys.getenv("run_Environment") == "development")) {
    loggit("INFO", "Preprocessed CohortID", app = "PhenoType Interface Functions")
    loggit("INFO", paste0(CohortID), app = "PhenoType Interface Functions")
  }

  # if a database has not been selected, choose OPTUM_EHR
  if (is.null(selectedDatabase)) {
    selectedDatabase <- "OPTUM_EHR"
  }
  if (as.character.default(Sys.getenv("run_Environment") == "development")) {
    loggit("INFO", paste(paste("CD_SQLITE", selectedDatabase, sep = "/"), "/", CohortID, ".sqlite", sep = ""), app = "PhenoType Interface Functions")
  }

  s3_key <- glue::glue("CD_SQLITE/{selectedDatabase}/{CohortID}.sqlite")
  s3_key_exists <- aws.s3::object_exists(s3_key, bucket = Sys.getenv("S3_bucket_cdsqlite_path"), region = Sys.getenv("S3_bucket_region"))

  if (isTRUE(!force_evaluation && s3_key_exists)) {
    logger::log_info("(sqlite_path) ID previously processed")
    # Download the file from S3 bucket
    sqliteDbPath <- glue::glue("{export_dir}/{selectedDatabase}/{CohortID}.sqlite")
    save_object(
      s3_key,
      file = sqliteDbPath,
      bucket = Sys.getenv("S3_bucket_server")
    )

    .GlobalEnv$sqliteDbPath <- sqliteDbPath
    sqliteDbPath
  } else { # Start of Single Cohort New Id loop



    logger::log_info("(sqlite_path) Processing new cohort(s)")
    if (grepl("_", CohortID)) {
      CohortID <- strsplit(as.character(CohortID), "_")[[1]]
    }
  } # End of Single Cohort New Id Loop

  # Arguments to create shinysettings,connectionhandlers,datasource
  sqliteDbPath <- sqliteDbPath
  vocabularyDatabaseSchemas <- "main"
  resultsDatabaseSchema <- "main"
  aboutText <- NULL
  tablePrefix <- ""
  cohortTableName <- "cohort"
  databaseTableName <- "database"
  enableAnnotation <- TRUE
  enableAuthorization <- FALSE

  if (as.character.default(Sys.getenv("run_Environment") == "development")) {
    loggit("INFO", "Value of tablePrefix: ", app = "PhenoType Interface Functions")
    loggit("INFO", paste(tablePrefix), app = "PhenoType Interface Functions")
  }

  tryCatch(
    { # SqliteDbPath
      # sqliteDbPath <- normalizePath(sqliteDbPath)
      # print(paste("sqliteDbPath: ", sqliteDbPath))
      # Establishing connection with the sqlite file
      connectionDetails <-
        DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = sqliteDbPath)
      print(connectionDetails)
      print(paste("sqliteDbPath", sqliteDbPath))
      # Shiny settings
      shinySettings <- list(
        connectionDetails = connectionDetails,
        resultsDatabaseSchema = resultsDatabaseSchema,
        vocabularyDatabaseSchemas = vocabularyDatabaseSchemas,
        aboutText = aboutText,
        tablePrefix = tablePrefix,
        cohortTableName = cohortTableName,
        databaseTableName = databaseTableName,
        enableAnnotation = enableAnnotation,
        enableAuthorization = FALSE
      )
      .GlobalEnv$shinySettings <- shinySettings
      print(paste("shiny :----", .GlobalEnv$shinySettings))
      print(shinySettings$connectionDetails)
      # Connectionhandler
      connectionHandler <-
        ResultModelManager::PooledConnectionHandler$new(shinySettings$connectionDetails)
      .GlobalEnv$connectionHandler <- connectionHandler
      print(.GlobalEnv$connectionHandler)
      # dataSource
      resultDatabaseSettings <- list(
        schema = as.character(shinySettings$resultsDatabaseSchema),
        vocabularyDatabaseSchema = shinySettings$vocabularyDatabaseSchema,
        cdTablePrefix = shinySettings$tablePrefix,
        cgTable = shinySettings$cohortTableName,
        databaseTable = shinySettings$databaseTableName
      )
      .GlobalEnv$resultDatabaseSettings <- resultDatabaseSettings

      dataSource <- OhdsiShinyModules::createCdDatabaseDataSource(
        connectionHandler = connectionHandler,
        resultDatabaseSettings = resultDatabaseSettings
      )
      .GlobalEnv$dataSource <- dataSource
    },
    error = function(e) {
      message("an error occurred connection details: ", e$message)
    }
  )

  # Removes the sqlite file that got downloaded, during the generation process, to free-up the memory space
  if (unlink_outputs) {
    unlink(sqliteDbPath, force = TRUE)
    unlink(paste0(CohortID, ".sqlite"), force = TRUE)
    unlink(export_dir, force = TRUE)
    unlink(paste(export_dir, selectedDatabase, sep = "/"), force = TRUE)
  }


  # Returns the arguments required to run the server portion of Cohort Diagnostics App
  return(
    list(
      connectionHandler = connectionHandler,
      resultDatabaseSettings = resultDatabaseSettings,
      shinySettings = shinySettings,
      dataSource = dataSource
    )
  )
}


#------------------------------------------------------------------------------------------------------------------------------
# Add the new function here... generate_sqlite_filepath_from_database

#' Function to retrieve data based on cohort ID and database ID ---------------
#'
#' @param schema_name
#' @param cohort_id
#' @param database_id
#' @param folder_path
#'
#' @return
#' @export
#'
download_csvs <- function(schema_name, cohort_id, database_id, folder_path) {
  # Query to get all tables in the schema
  query_tables <- paste0("SELECT table_name FROM information_schema.tables WHERE table_schema = '", schema_name, "';")
  logger::log_info("query_tables: ", query_tables)
  tables <- run_query_RW(query_tables)
  logger::log_info("tables: ", toString(tables, Inf))
  # Create the folder if it doesn't exist
  if (!dir.exists(folder_path)) {
    dir.create(folder_path, recursive = TRUE)
  }

  for (table in tables$table_name) {
    if (!(table %in% c("phenotype_details", "phenotype_clinical_code_list", "requested_phenotype_clinical_code_list", "requested_phenotype_details", "requested_phenotype_response", "cohort_censor_stats", "user_sessions"))) {
      logger::log_info(
        glue::glue("({which(table == tables$table_name)}/{length(tables$table_name)}) Downloading table: {table}")

      )
      query <- paste0(
        "SELECT * FROM ", schema_name, ".", table,
        " WHERE cohort_id in (", paste0(cohort_id, collapse = ", "), ") AND database_id in (", paste0(glue::glue("'{database_id}'"), collapse = ", "), ");"
      )
      logger::log_info("(download_csvs) query: {query}")
      data <- run_query_RW(query)

      if (nrow(data) > 0) {
        if (table == "cohort_inc_results") {
          table <- "cohort_inc_result"
        } else if (table == "concept_synonyms") {
          table <- "concept_synonym"
        } else if (table == "executiontimes") {
          table <- "executionTimes"
        } else if (table == "orphan_concepts") {
          table <- "orphan_concept"
        } else {
          table
        }
        logger::log_info("Renamed table: ", table)

        file_name <- paste0(folder_path, "/", table, ".csv")
        if (file.exists(file_name)) {
          # Append to existing CSV
          readr::write_csv(data, file_name, append = TRUE)
        } else {
          # Write new CSV
          readr::write_csv(data, file_name)
        }
      }
    } else {
      logger::log_info("tableloop: ", table)
    }
  }
}
# End of download_csvs function

#' process_cohort_data
#'
#' Main function to handle cohort data from database
#'
#' @param schema_name
#' @param CohortID
#' @param multiple_databaseCodes
#'
#' @return
#' @export
#'
process_cohort_data <- function(schema_name, CohortID, multiple_databaseCodes, export_dir) {
  checkmate::assert_directory_exists(export_dir)

  cohort_ids <- unlist(strsplit(CohortID, "_"))
  logger::log_info(paste("cohort_ids: ", toString(cohort_ids, Inf)))
  cohort_id_list <- paste0(paste(cohort_ids, collapse = ","))
  logger::log_info("ID previously processed")
  exportFolder <- paste(export_dir, "merged", multiple_databaseCodes, sep = "/")
  logger::log_info(paste("exportFolder:", exportFolder))

  databases <- parse_multiple_databaseCodes(multiple_databaseCodes)

  download_csvs(schema_name, cohort_ids, databases, exportFolder)

  # Change directory to the folder containing csvs
  current_dir <- getwd()
  setwd(exportFolder)
  # Create a zip file for the csvs without including folder path
  zip_file <- paste0(exportFolder, "/", "Results_", multiple_databaseCodes, ".zip")
  logger::log_info(paste("zip_file:", zip_file))
  zip(basename(zip_file), files = list.files(pattern = "*.csv"))

  # Change back to original working directory
  setwd(current_dir)

  # exportFolder <- paste(export_dir, "merged", multiple_databaseCodes, sep = "/")
  sqliteDbPath <- paste(exportFolder, "/", paste(sort(CohortID), collapse = "_"), ".sqlite", sep = "")
  # sqliteDbPath <- paste(exportFolder, paste0(paste0(CohortID, collapse='_'), ".sqlite"), sep = "/")
  # unlink(file.path(getwd(),"export","merged", multiple_databaseCodes,"*.csv"), force = TRUE)

  tryCatch(
    {
      CohortDiagnostics::createMergedResultsFile(exportFolder,
                                                 sqliteDbPath = sqliteDbPath,
                                                 overwrite = TRUE
      )
      logger::log_success("Created sqlite file from database: ", sqliteDbPath)

      append_correlation_analysis_tables_to_sqlite(exportFolder, sqliteDbPath)

      return(list(
        sqliteDbPath = sqliteDbPath,
        exportFolder = exportFolder
      ))
    },
    error = function(e) {
      message("an error occurred in the if condition: ", e$message)
    }
  )
}
# End of process_cohort_data function


#' generate_sqlite_filepath_from_database
#'
#' @param scratch_space_name
#' @param username
#' @param password
#' @param atlas_user_name
#' @param atlas_password
#' @param CohortID
#' @param server
#' @param schema_name
#' @param multiple_databaseCodes
#'
#' @return
#' @export
#'
generate_sqlite_filepath_from_database <- function(scratch_space_name,
                                                   username,
                                                   password,
                                                   atlas_user_name,
                                                   atlas_password,
                                                   CohortID,
                                                   server,
                                                   schema_name,
                                                   multiple_databaseCodes,
                                                   export_dir) {
  logger::log_info("(generate_sqlite_filepath_from_database) Process Cohort Data function call. Download data from database")
  results <- process_cohort_data(schema_name, CohortID, multiple_databaseCodes, export_dir)

  checkmate::assert_names(names(results), must.include = c("sqliteDbPath", "exportFolder"))
  checkmate::assert_file_exists(results$sqliteDbPath)
  checkmate::assert_directory_exists(results$exportFolder)

  logger::log_success("Download data from database - completed")
  results
} # End of generate_sqlite_filepath_from_database function


#' sqlite_path_multiple
#'
#' Function to generate sqlite path for selected multiple databases
#'
#' @param CohortID
#' @param selectedDatabase
#' @param multiple_databaseCodes
#' @param export_dir
#' @param local_sqlite_path
#' @param force_evaluation
#' @param send_notification_email
#' @param session_user
#' @param schema_name
#'
#' @return
#' @export
#'
sqlite_path_multiple <- function(CohortID = NULL,
                                 selectedDatabase = NULL,
                                 multiple_databaseCodes = NULL,
                                 export_dir = file.path(getwd(), "export"),
                                 local_sqlite_path = Sys.getenv("local_sqlite_path"),
                                 ExistingCohortBit = FALSE,
                                 force_evaluation = FALSE,
                                 send_notification_email = FALSE,
                                 session_user = NULL,
                                 schema_name = "phenotype_library") {
  
  checkmate::assert_string(local_sqlite_path, min.chars = 1)
  
  if (send_notification_email && !shiny::isTruthy(session_user)) {
    stop("send_notification_email is set to TRUE, so valid session_user must be provided.")
  }
  
  FUN_OUTPUTS <- list()
  
  # Crear directorio de almacenamiento si no existe
  if (!dir.exists(local_sqlite_path)) {
    dir.create(local_sqlite_path, recursive = TRUE)
  }
  
  # Formatear Cohort IDs
  CohortID <- sort.default(CohortID)
  logger::log_info(paste("Sorted Cohort Ids :", CohortID))
  
  # Ruta local del archivo SQLite
  db_folder <- file.path(local_sqlite_path, selectedDatabase)
  if (!dir.exists(db_folder)) {
    dir.create(db_folder, recursive = TRUE)
  }
  
  local_sqlite_file <- file.path(db_folder, paste0(CohortID, ".sqlite"))
  
  # Verificar si el archivo ya existe localmente
  if (file.exists(local_sqlite_file) && !force_evaluation && ExistingCohortBit) {
    logger::log_info("(sqlite_path_multiple) Pulling from local storage - cohort previously processed")
    
    sqliteDbPath <- local_sqlite_file
    FUN_OUTPUTS$pulled_from_database <- TRUE
    FUN_OUTPUTS$sqliteDbPath <- sqliteDbPath
    
  } else {
    # Procesar nuevos cohorts
    logger::log_info("(sqlite_path_multiple) Processing new cohort(s)")
    
    results <- generate_sqlite_filepath(
      CohortID = CohortID,
      selectedDatabase = selectedDatabase,
      multiple_databaseCodes = multiple_databaseCodes,
      export_dir = export_dir,
      local_sqlite_path = local_sqlite_path
    )
    
    sqliteDbPath <- results$sqliteDbPath
    FUN_OUTPUTS$sqliteDbPath <- sqliteDbPath
    FUN_OUTPUTS$exportFolder <- results$exportFolder
  }
  
  # Establecer conexión con SQLite
  tryCatch(
    {
      connectionDetails <- DatabaseConnector::createConnectionDetails(
        dbms = "sqlite", 
        server = sqliteDbPath
      )
      
      shinySettings <- list(
        connectionDetails = connectionDetails,
        resultsDatabaseSchema = "main",
        vocabularyDatabaseSchemas = "main",
        aboutText = NULL,
        tablePrefix = "",
        cohortTableName = "cohort",
        databaseTableName = "database",
        enableAnnotation = TRUE,
        enableAuthorization = FALSE
      )
      
      .GlobalEnv$shinySettings <- shinySettings
      
      connectionHandler <- ResultModelManager::PooledConnectionHandler$new(
        shinySettings$connectionDetails
      )
      .GlobalEnv$connectionHandler <- connectionHandler
      
      resultDatabaseSettings <- list(
        schema = "main",
        vocabularyDatabaseSchema = "main",
        cdTablePrefix = "",
        cgTable = "cohort",
        databaseTable = "database"
      )
      .GlobalEnv$resultDatabaseSettings <- resultDatabaseSettings
      
      dataSource <- OhdsiShinyModules::createCdDatabaseDataSource(
        connectionHandler = connectionHandler,
        resultDatabaseSettings = resultDatabaseSettings
      )
      .GlobalEnv$dataSource <- dataSource
      
      FUN_OUTPUTS$connectionHandler <- connectionHandler
      FUN_OUTPUTS$resultDatabaseSettings <- resultDatabaseSettings
      FUN_OUTPUTS$shinySettings <- shinySettings
      FUN_OUTPUTS$dataSource <- dataSource
      
    },
    error = function(e) {
      logger::log_error("Error establishing connection: ", e$message)
    }
  )
  
  # Enviar notificación por email si es requerido
  if (send_notification_email && file.exists(sqliteDbPath)) {
    send_mail_sqlite_local(
      local_file_path = sqliteDbPath,
      CohortID = CohortID,
      databases = selectedDatabase,
      session_user = session_user
    )
  }
  
  return(FUN_OUTPUTS)
}
