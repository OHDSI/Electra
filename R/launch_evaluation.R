# datatable_rows_selected  # Original: input$datatable_rows_selected
# dataBase                 # Original: dataBase
# CohortID                 # Original: CohortID
# atlas_user_name          # Original: atlas_user_name
# atlas_password           # Original: atlas_password
# phenotype_details        # Original: input$phenotype_details
# PL_app_dbs               # Original: PL_app_dbs (likely an object or parameter already in the environment)
# S3_bucket_server         # Original: input$S3_bucket_server
# S3_bucket_cdsqlite_path  # Original: input$S3_bucket_cdsqlite_path
# S3_bucket_region         # Original: input$S3_bucket_region
# run_Environment          # Original: input$run_Environment
# redshift_username        # Original: input$redshift_username
# redshift_password        # Original: input$redshift_password
# session_user             # Original: session_user


#' launch_cohort_evaluation
#'
#' @param datatable_rows_selected
#' @param dataBase
#' @param CohortID
#' @param atlas_user_name
#' @param atlas_password
#' @param home_page_df
#' @param phenotype_details
#' @param PL_app_dbs
#' @param S3_bucket_server
#' @param S3_bucket_cdsqlite_path
#' @param S3_bucket_region
#' @param run_Environment
#' @param redshift_username
#' @param redshift_password
#' @param session_user
#' @param session
#' @param unlink_outputs
#' @param export_dir
#' @param force_evaluation
#'
#' @export
#'
launch_cohort_evaluation <- function(
    datatable_rows_selected,
    dataBase,
    CohortID,
    atlas_user_name,
    atlas_password,
    home_page_df,
    phenotype_details,
    PL_app_dbs,
    S3_bucket_server,
    S3_bucket_cdsqlite_path,
    S3_bucket_region,
    run_Environment,
    redshift_username,
    redshift_password,
    session_user,
    session = NULL,
    unlink_outputs = FALSE,
    export_dir = file.path(getwd(), "export"),
    force_evaluation = FALSE,
    drivers_dir = paste(getwd(), "drivers", sep = "/"),
    postgresql_schema_name = "phenotype_library") {

  logger::log_info("(launch_cohort_evaluation) Validating arguments...")
  stopifnot(
    !missing(datatable_rows_selected),
    !missing(dataBase),
    !missing(CohortID),
    !missing(atlas_user_name),
    !missing(atlas_password),
    !missing(home_page_df),
    !missing(phenotype_details),
    !missing(PL_app_dbs),
    !missing(S3_bucket_server),
    !missing(S3_bucket_cdsqlite_path),
    !missing(S3_bucket_region),
    !missing(run_Environment),
    !missing(redshift_username),
    !missing(redshift_password),
    !missing(session_user),
    !missing(postgresql_schema_name)
  )

  FUN_OUTPUTS <- list()

  selected_phenotype <- home_page_df[datatable_rows_selected, ]$`Phenotype Name`
  selected_row <- subset(phenotype_details, phenotype_name %in% selected_phenotype)

  logger::log_info(glue::glue("selected_databases: {paste0(dataBase, collapse = ', ')}"))
  logger::log_info(glue::glue("force_evaluation: {force_evaluation}"))

  cohort_id_list <- character()
  # Get the selected Multiple Database values
  if (!is.character(dataBase)) {
    stop("dataBase is not character string")
  } else {
    # Set the values for selected multiple databases
    selected_databases <- unlist(strsplit(dataBase, ","))
    if (length(selected_databases) > 1) {
      (IsMultipleDB <- TRUE)
    } else {
      (IsMultipleDB <- FALSE)
      selected_database <- dataBase
    }
    logger::log_info("selected databases: ", toString(selected_databases, Inf))
  }
  logger::log_info("IsMultipleDB: ", IsMultipleDB)
  # Get the values for entered cohort ids
  cohort_id_text <- gsub("_COMMA_", ",", CohortID)
  if (!is.null(cohort_id_text) && nzchar(cohort_id_text)) {
    cohort_id_list <- strsplit(cohort_id_text, ",")[[1]]
    logger::log_info("selected cohort ids: ", toString(cohort_id_list, Inf))
  }

  codes <- get_database_codes(selected_databases, PL_app_dbs)
  multiple_databaseCodes <- codes$multiple_databaseCodes
  multiple_dbCodes <- codes$multiple_dbCodes
  multiple_databaseCodes_count <- codes$multiple_databaseCodes_count

  # Cohort ID
  cohort_id <- character()
  cohort_table <- character()
  # postgresql_schema_name <- character()
  for (co in seq_along(cohort_id_list))
  { # start of cohort id For Loop

    cohort_id_num <- as.numeric(cohort_id_list[co])
    cohort_id <- c(cohort_id, cohort_id_num)
  }

  # check if SQLite File already exists. If not, start progress bar.

  bucket <- S3_bucket_server
  bucket_contents <- aws.s3::get_bucket_df(
    bucket = S3_bucket_cdsqlite_path,
    region = S3_bucket_region
  )
  # Preprocess cohort_id if multiple IDs were present sort and format
  logger::log_info("cohort_id: ", toString(cohort_id, Inf))
  # Check if Single Cohort entered
  if (length(cohort_id) > 1) {
    cohort_id <- paste(sort(cohort_id), collapse = "_")
  } else {
    cohort_id <- cohort_id
  }
  logger::log_info("cohort_id after collapse: ", cohort_id)
  if (IsMultipleDB == TRUE) {
    db_name <- multiple_databaseCodes
  } else {
    db_name <- selected_database
  }
  cohort_table <- get_cohort_table_for_evaluation()
  loggit("INFO", paste("cohort_table:", cohort_table))
  loggit("INFO", paste("cohort_id", cohort_id))
  loggit("INFO", paste("multiple_dbCodes", multiple_dbCodes))

  if (is.na(CohortID == FALSE) || CohortID != "") {
    comb_exists <- isTRUE(combination_exists(postgresql_schema_name, cohort_table, cohort_id, multiple_dbCodes) == "t")

    if (comb_exists && !force_evaluation) {
      logger::log_info("Check if ID previously processed")
      ExistingCohortBit <- TRUE
    } else {
      ExistingCohortBit <- FALSE
      logger::log_info("Combination doesn't exist")
    }
  }

  loggit("INFO", "Loop through each selected Database")
  for (i in seq_along(selected_databases))
  { # Start of database For Loop
    db <- selected_databases[i]

    logger::log_info("Selected Database: ", db)
    selectedDb_row <- subset(PL_app_dbs, Name == db)
    selected_databaseCode <- selectedDb_row$db_code
    rhealth_server <- selectedDb_row$rHealth_ServerName
    scratch_space_name <- selectedDb_row$Scratch_Space_Name
    selected_database <- selected_databaseCode
    logger::log_info("db_count: ", i)

    if (as.character.default(run_Environment == "development")) {
      loggit("INFO", paste0("selected_databases: ", selected_databases[i]), app = "launch_cohort_evaluation.R")
      loggit("INFO", paste0("rhealth_server: ", rhealth_server), app = "launch_cohort_evaluation.R")
      loggit("INFO", paste0("selected_databaseCode: ", selected_databaseCode), app = "launch_cohort_evaluation.R")
      loggit("INFO", paste0("scratch_space_name: ", scratch_space_name), app = "launch_cohort_evaluation.R")
    }
    # The below if/else block returns the arguments required to run the server part of Cohort Diagnostics App [cohortDiagnosticsServer()]
    # Logging only for dev run environment
    if (as.character.default(run_Environment == "development")) {
      loggit("INFO", paste0("cohort_id_list: ", toString(cohort_id_list, Inf)), app = "launch_cohort_evaluation.R")
    }

    if (is.na(CohortID) || CohortID == "") {
      if (as.character.default(run_Environment == "development")) {
        loggit("INFO", "CohortID is NULL Get ID from ATLAS cohort Definition: ", app = "launch_cohort_evaluation.R")
      }
      cohort_id <- as.numeric(selected_row$jnj_cohort_definition_id)
      loggit("INFO", paste0("cohort_id: ", cohort_id), app = "launch_cohort_evaluation.R")

      sqlite_path(
        CohortID = cohort_id,
        selectedDatabase = selected_databaseCode,
        unlink_outputs = unlink_outputs,
        export_dir = export_dir,
        force_evaluation = force_evaluation
      )
      if (as.character.default(run_Environment == "development")) {
        loggit("INFO", paste0(cohort_id), app = "launch_cohort_evaluation.R")
      }
    } else { # start of else condition
      # Loop through each cohort id for current database
      logger::log_info("Cohort ID Loop")
      cohort_id <- character()
      for (j in seq_along(cohort_id_list)) { # start of cohort id For Loop

        cohort_id_num <- as.numeric(cohort_id_list[j])
        selected_database <- selected_databaseCode
        loggit("INFO", "current cohort_id: ", app = "launch_cohort_evaluation.R")
        loggit("INFO", cohort_id_num, app = "launch_cohort_evaluation.R")
        logger::log_info("selected_database: ", selected_database)

        # If a user supplies a cohort ID along with additional credentials, the provided cohort ID is then passed to the "sqlite_path" function along with the other credentials
        # if (!is.na(CohortID_Optional)) {
        if (as.character.default(run_Environment == "development")) {
          loggit("INFO", "MULTIPLE CohortIDs Entered: ", app = "launch_cohort_evaluation.R")
        }

        #  cohort_id <- c(CohortID, CohortID_Optional)
        cohort_id <- c(cohort_id, cohort_id_num)
        if (as.character.default(run_Environment == "development")) {
          loggit("INFO", as.character(cohort_id), app = "launch_cohort_evaluation.R")
        }
      } # end of cohort id loop
      logger::log_info("cohort id after loop: ", toString(cohort_id, Inf))
      logger::log_info("selected_database: ", toString(selected_database, Inf))


      if (as.character.default(run_Environment == "development")) {
        loggit("INFO", "START EXEC Sqlite_Path funciton: ", app = "launch_cohort_evaluation.R")
      }

      tryCatch(
        {
          logger::log_info("TRY")




          # check if SQLite File already exists. If not, start progress bar.

          bucket <- S3_bucket_server
          bucket_contents <- aws.s3::get_bucket_df(
            bucket = S3_bucket_cdsqlite_path,
            region = S3_bucket_region
          )
          # Preprocess cohort_id if multiple IDs were present sort and format
          cohort_id <- paste(sort(cohort_id), collapse = "_")
          logger::log_info("cohort_id after collapse: ", toString(cohort_id, Inf))
          if (IsMultipleDB == TRUE) {
            logger::log_info("MultipleDB is true")
            db_name <- multiple_databaseCodes
          } else {
            db_name <- selected_database
          }

          if (ExistingCohortBit == TRUE) {
            logger::log_info(
              "ID previously processed: ",
              toString(paste(paste("CD_SQLITE", "Merged", db_name, sep = "/"), "/", cohort_id, ".sqlite", sep = ""), Inf))

            #       if(IsMultipleDB == TRUE)
            #      {
            # Show a spinning wheel while Processing
            if (!is.null(session)) {
              show_modal_spinner(spin = "circle", text = "Retrieving data from database...")
            }

            FUN_OUTPUTS[[1]] <- sqlite_path_multiple(
              CohortID = cohort_id,
              scratch_space_name = scratch_space_name, # Sys.getenv("scratch_space_name"),
              username = redshift_username,
              password = redshift_password,
              atlas_user_name = atlas_user_name,
              atlas_password = atlas_password,
              server = rhealth_server,
              selectedDatabase = selected_database,
              multiple_databaseCodes = multiple_databaseCodes,
              total_db_count = multiple_databaseCodes_count,
              db_count = i,
              ExistingCohortBit = ExistingCohortBit,
              IsMultipleDB_Flag = IsMultipleDB,
              unlink_outputs = unlink_outputs,
              export_dir = export_dir,
              drivers_dir = drivers_dir,
              force_evaluation = force_evaluation,
              schema_name = postgresql_schema_name
            )

            FUN_OUTPUTS[[1]]$cohort_id <- cohort_id
            FUN_OUTPUTS[[1]]$ExistingCohortBit <- TRUE

            if (!is.null(session)) {
              remove_modal_spinner()
            }

            # no need to iterateover all DBS and Cohort because it will retrieve from Postgres DB
            # all cohorts and databases and merge them into one sqlite
            logger::log_info("Breaking the loop")
            break

          } else {
            logger::log_info("Processing new cohort(s)")

            if (!is.null(session)) {
              # Disable Launch button
              shinyjs::disable("Launch")


              numberOfSeconds <- 2700 * multiple_databaseCodes_count # 45 mins ## TODO MULTIPLY per Database per Cohort
              print(paste("numberOfSeconds", numberOfSeconds))
              startProgressTimer(numberOfSeconds * 1000, easing = "linear")
            }
            logger::log_info("redshift_username: ", redshift_username)
            logger::log_info("atlas_user_name: ", atlas_user_name)


            FUN_OUTPUTS[[db]] <- sqlite_path_multiple(
              CohortID = cohort_id,
              scratch_space_name = scratch_space_name,
              username = redshift_username,
              password = redshift_password,
              atlas_user_name = atlas_user_name,
              atlas_password = atlas_password,
              server = rhealth_server,
              selectedDatabase = selected_database,
              multiple_databaseCodes = multiple_databaseCodes,
              total_db_count = multiple_databaseCodes_count,
              db_count = i,
              ExistingCohortBit = ExistingCohortBit,
              IsMultipleDB_Flag = IsMultipleDB,
              unlink_outputs = unlink_outputs,
              export_dir = export_dir,
              drivers_dir = drivers_dir,
              force_evaluation = force_evaluation,
              schema_name = postgresql_schema_name,
              send_notification_email = TRUE,
              session_user = session_user
            )

            FUN_OUTPUTS[[db]]$cohort_id <- cohort_id

            logger::log_success("Function sqlite_path_multiple finished")

            # Enable Launch button
            if (!is.null(session)) {
              shinyjs::enable("Launch")

              shinyjs::click("phenotype_Definition")
              shinyjs::click("cohortDefinition")
            }

          }
        },
        error = function(msg) {
          logger::log_error("Error with Generating SQLite Path: ", toString(msg, Inf))

          if (!is.null(session)) {
            # Close the Progress Bar
            closeProgressTimer()

            # Enable Launch button to allow for relaunch
            shinyjs::enable("Launch")
          }

          if (grepl("Assertion on 'webApiUsername' failed", msg)) {
            session %&&% showModal(modalDialog(
              title = "Error with Processing your Cohort(s)",
              paste("Please ensure you have entered your ATLAS Credentials.
                  Error: ", "/n", msg),
              easyClose = TRUE,
              footer = NULL,
            ))
          } else if (grepl("Authentication", msg)) {
            session %&&% showModal(modalDialog(
              title = "Error with Processing your Cohort(s)",
              paste("Please ensure you are using the correct ATLAS Credentials.
                  Error: ", "/n", msg),
              easyClose = TRUE, footer = NULL
            ))
          } else if (grepl("not found", msg)) {
            session %&&% showModal(modalDialog(
              title = "Error with Processing your Cohort(s)",
              paste("Please ensure you are using a valid Cohort that exists in ATLAS.
                  Error: ", "/n", msg),
              easyClose = TRUE,
              footer = NULL
            ))
          } else {
            session %&&% showModal(modalDialog(
              title = "Error with Processing your Cohort(s)",
              paste("Please check the errors and try again.
                  Error: ", "/n", msg),
              easyClose = TRUE,
              footer = NULL
            ))
          }
        }
      )


      if (as.character.default(run_Environment == "development")) {
        loggit("INFO", "END EXEC Sqlite_Path funciton: ", apps = "launch_cohort_evaluation.R")
      }
    } # end of else condition
  } # end of For loop for databases
  if (!is.null(session)) {
    # Close the Progress Bar
    closeProgressTimer()
    toast("Execution Completed!", type = "success")
  }

  return(FUN_OUTPUTS)
} # end of launch
