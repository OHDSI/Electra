

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
#' Function to generate sqlite path for selected multiple databases retrieving from postfresql database
#'
#' @param scratch_space_name
#' @param username
#' @param password
#' @param atlas_user_name
#' @param atlas_password
#' @param CohortID
#' @param server
#' @param selectedDatabase
#' @param multiple_databaseCodes
#' @param total_db_count
#' @param db_count
#' @param ExistingCohortBit
#' @param IsMultipleDB_Flag
#'
#' @return
#' @export
#'
sqlite_path_multiple <- function(scratch_space_name = NULL,
                                 username = NULL,
                                 password = NULL,
                                 atlas_user_name = NULL,
                                 atlas_password = NULL,
                                 CohortID = NULL,
                                 server = NULL,
                                 selectedDatabase = NULL,
                                 multiple_databaseCodes = NULL,
                                 total_db_count = 0,
                                 db_count = 0,
                                 ExistingCohortBit = NULL,
                                 IsMultipleDB_Flag = NULL,
                                 unlink_outputs = TRUE,
                                 export_dir = file.path(getwd(), "export"),
                                 drivers_dir = paste(getwd(), "drivers", sep = "/"),
                                 force_evaluation = FALSE,
                                 schema_name = "phenotype_library",
                                 send_notification_email = FALSE,
                                 session_user = NULL) { # Start of sqlite_path_multiple function

  checkmate::assert_directory_exists(drivers_dir)
  checkmate::assert_logical(force_evaluation)

  if (send_notification_email && !shiny::isTruthy(session_user)) {
    stop("send_notification_email is set to TRUE, so valid session_user must be provided.")
  }

  FUN_OUTPUTS <- list()

  if (as.character.default(Sys.getenv("run_Environment") == "development")) {
    loggit("INFO", paste0("CohortID: ", as.character.default(CohortID)), app = "PhenoType Interface Functions")
    loggit("INFO", paste0("CohortID[1]: ", CohortID[1]), app = " PhenoType Interface Functions")
    loggit("INFO", paste0("CohortID[2]: ", CohortID[2]), app = " PhenoType Interface Functions")
  }

  # sort the Cohorts when multiple to avoid creating mutiple files for the same cohorts regardless of the order inputed
  CohortID <- sort.default(CohortID)
  logger::log_info(paste("Sorted Cohort Ids :", CohortID))


  #--------------------------------------ADD CODE HERE ------------------------------------------#

  # Add code to pull data for selected DB and Cohorts from App database and into CSV files
  # Zip up the csv files for this DB and do same for other selected DBs
  # Get the location of all ZIP files and set it as the Export Folder

  # Set Export Folder after determining which DBs and cohorts we are generating the file for
  # Check if Cohort Ids already exist then retrieve data from database
  if (ExistingCohortBit == TRUE && force_evaluation == FALSE) {
    logger::log_info("Pulling from database Cohort IDs previously processed")
    # schema_name <- "phenotype_library"

    results <- generate_sqlite_filepath_from_database(
      scratch_space_name = scratch_space_name,
      username = username,
      password = password,
      atlas_user_name = atlas_user_name,
      atlas_password = atlas_password,
      CohortID = CohortID,
      server = server,
      schema_name = schema_name,
      multiple_databaseCodes = multiple_databaseCodes,
      export_dir = export_dir
    )

    sqliteDbPath <- results$sqliteDbPath
    logger::log_success("sqliteDbPath: Retrieved from Database")
    .GlobalEnv$sqliteDbPath <- sqliteDbPath
    FUN_OUTPUTS$pulled_from_database <- TRUE
    FUN_OUTPUTS$sqliteDbPath <- sqliteDbPath
    # folder_path = paste0(getwd(),"/export/merged/", multiple_databaseCodes,"/")
    # sqliteDbPath = paste(folder_path,"/", CohortID, ".sqlite", sep = "")
  } else { # Start of Processing New Cohorts Loop
    # Else process new Cohort Ids
    logger::log_info("Processing new cohort(s)")
    # if(grepl('_', CohortID)){
    # Cohort_ID = strsplit(as.character(CohortID), '_')[[1]]
    # }

    results <- generate_sqlite_filepath(
      scratch_space_name = scratch_space_name,
      username = username,
      password = password,
      atlas_user_name = atlas_user_name,
      atlas_password = atlas_password,
      CohortID = CohortID,
      server = server,
      multiple_databaseCodes = multiple_databaseCodes,
      selectedDatabase = selectedDatabase,
      export_dir = export_dir,
      drivers_dir = drivers_dir,
      send_notification_email = send_notification_email,
      session_user = session_user
    )

    checkmate::assert_names(names(results), must.include = c("sqliteDbPath", "exportFolder"))
    sqliteDbPath <- results$sqliteDbPath
    .GlobalEnv$sqliteDbPath <- sqliteDbPath
    exportFolder <- results$exportFolder

    FUN_OUTPUTS$sqliteDbPath <- sqliteDbPath
    FUN_OUTPUTS$exportFolder <- exportFolder

    # Save csv to database server
    zip_file_path <- paste0(exportFolder, "/", "Results_", selectedDatabase, ".zip")
    logger::log_info(glue::glue("Compressing CSV files from {exportFolder} to {zip_file_path}"))
    local({
      wd <- getwd()
      on.exit(setwd(wd))
      setwd(exportFolder)
      logger::log_info("zip_file_path:", zip_file_path)
      zip(basename(zip_file_path), files = list.files(pattern = "*.csv"))
    })

    FUN_OUTPUTS$zip_file_path <- zip_file_path
    logger::log_success("Zip is ready: ", zip_file_path)
    logger::log_info("exportFolder: ", exportFolder)

    # Call function
    logger::log_info(glue::glue("Uploading CSVs from '{zip_file_path}' to PostgreSQL..."))
    upload_csvs_from_zip_to_db(
      zip_file_path = zip_file_path,
      CohortID = CohortID,
      selectedDatabase =  selectedDatabase,
      overwrite = FALSE,
      append = TRUE,
      tablePrefix = schema_name
    )

    logger::log_success("Upload PostgreSQL is done")

    if (send_notification_email) {
      send_mail_zip_to_database(CohortID = CohortID, databases = selectedDatabase, session_user = session_user)
    }

  } # End of Processing New Cohorts Loop


  # print(paste("total_db_count = db_count :", total_db_count, db_count))

  # Arguments to create shinysettings,connectionhandlers,datasource
  # sqliteDbPath <- sqliteDbPath
  # print("sqliteDbPath: Saving to Database")
  # print(sqliteDbPath)
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

  # tryCatch({
  # sqliteDbPath <- normalizePath(sqliteDbPath)
  # print(paste("Start normalized SqliteDbPath ", sqliteDbPath))
  # sqliteDbPath = normalizePath(sqliteDbPath)
  # print(paste("End normalized SqliteDbPath ", sqliteDbPath))
  # }, error = function(e)
  # {
  # message("an error occurred in normalizePath: ", e$message)
  # }
  # )
  # )

  tryCatch(
    {
      # Establishing connection with the sqlite file
      connectionDetails <-
        DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = sqliteDbPath)
      print(connectionDetails)
      # }
      # Shiny settings
      .GlobalEnv$shinySettings <- list(
        # if(total_db_count == db_count | ExistingCohortBit == TRUE){
        connectionDetails = connectionDetails,
        # },
        resultsDatabaseSchema = resultsDatabaseSchema,
        vocabularyDatabaseSchemas = vocabularyDatabaseSchemas,
        aboutText = aboutText,
        tablePrefix = tablePrefix,
        cohortTableName = cohortTableName,
        databaseTableName = databaseTableName,
        enableAnnotation = enableAnnotation,
        enableAuthorization = FALSE
      )
      print(paste("shiny :----", .GlobalEnv$shinySettings))
      print(shinySettings$connectionDetails)
      # Connectionhandler
      # if(total_db_count == db_count | ExistingCohortBit == TRUE){
      connectionHandler <-
        ResultModelManager::PooledConnectionHandler$new(shinySettings$connectionDetails)
      .GlobalEnv$connectionHandler <- connectionHandler
      # }
      print(.GlobalEnv$connectionHandler)
      # dataSource
      .GlobalEnv$resultDatabaseSettings <- list(
        schema = as.character(shinySettings$resultsDatabaseSchema),
        vocabularyDatabaseSchema = shinySettings$vocabularyDatabaseSchema,
        cdTablePrefix = shinySettings$tablePrefix,
        cgTable = shinySettings$cohortTableName,
        databaseTable = shinySettings$databaseTableName
      )
      .GlobalEnv$dataSource <-
        OhdsiShinyModules::createCdDatabaseDataSource(
          connectionHandler = connectionHandler,
          resultDatabaseSettings = resultDatabaseSettings
        )
    },
    error = function(e) {
      message("an error occurred connection details: ", e$message)
    }
  )

  # Removes the sqlite file that got downloaded, during the generation process, to free-up the memory space
  if (unlink_outputs) {
    # unlink(sqliteDbPath, force = TRUE)
    unlink(paste0(CohortID, ".sqlite"), force = TRUE)
    unlink(export_dir, force = TRUE)
    unlink(paste(export_dir, selectedDatabase, sep = "/"), force = TRUE)
  }

  # Removes the sqlite file that got downloaded for multiple databases
  #  unlink(paste(getwd(), "export","merged", multiple_databaseCodes, sep = "/"), force = TRUE)

  # Returns the arguments required to run the server portion of Cohort Diagnostics App
  logger::log_info("list:---")

  FUN_OUTPUTS$connectionHandler <- connectionHandler
  FUN_OUTPUTS$resultDatabaseSettings <- resultDatabaseSettings
  FUN_OUTPUTS$shinySettings <- shinySettings
  FUN_OUTPUTS$dataSource <- dataSource

  return(FUN_OUTPUTS)
} # End of sqlite_path_multiple function
