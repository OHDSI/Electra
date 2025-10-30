# args <- commandArgs(TRUE)
# CohortID <- args[1]

#' evaluate_cohort_against_databases
#'
#' @param CohortID
#' @param databases
#' @param databases_csv
#'
#' @importFrom loggit loggit
#'
#' @return
#' @export
#'
#' @importFrom dplyr `%>%`
#'
evaluate_cohort_against_databases <- function(CohortID,
                                              databases,
                                              session_user,
                                              atlas_password,
                                              atlas_user_name = session_user,
                                              databases_csv = "PL_App_Databases.csv",
                                              upload_to_s3 = FALSE,
                                              sink_log = FALSE,
                                              force_evaluation = FALSE) {
  checkmate::assert_scalar(CohortID)
  checkmate::assert_string(CohortID, min.chars = 1L)
  checkmate::assert_vector(databases, min.len = 1L, unique = TRUE, null.ok = FALSE)
  checkmate::assert_string(session_user, min.chars = 1L)
  checkmate::assert_string(atlas_password, min.chars = 1L)
  checkmate::assert_string(atlas_user_name, min.chars = 1L)
  checkmate::assert_file_exists(databases_csv)

  FUN_OUTPUT <- list()

  PL_app_dbs <- utils::read.csv(databases_csv)
  dataBase <- PL_app_dbs$Name

  databases <- match.arg(databases, choices = dataBase, several.ok = TRUE)

  loggit("INFO", paste0("Evaluating against dbs: ", toString(databases, width = Inf)), app = "evaluate_cohort_against_databases")

  logger::log_info("(evaluate_cohort_against_databases) CohortID: ", toString(CohortID, Inf))

  CohortID <- gsub("__COMMA__", ",", CohortID)

  logger::log_info("(evaluate_cohort_against_databases) CohortID fixed: ", toString(CohortID, Inf))

  if (grepl("__COMMA__", CohortID)) {
    logger::log_warn(glue::glue("(evaluate_cohort_against_databases) Removing '__COMMA__' from CohortID : '{CohortID}'"))
    CohortID <- gsub("__COMMA__", ",", CohortID)
  }

  # source("Phenotype_Interface_Functions.R")

  # source("credentials.R") # atlas_user_name, atlas_password
  # CohortID = "17445"


  if (file.access(getwd(), mode = 2) == 0) {
    root_dir <- getwd()
  } else {
    dir.create(root_dir <- tempfile())
  }

  export_dir <- file.path(root_dir, "export", paste0(CohortID, "-evaluation"), as.integer(Sys.time()))
  dir.create(export_dir, recursive = TRUE)
  loggit("INFO", paste0("Export dir: ", export_dir), app = "evaluate_cohort_against_databases")
  sql_error_report_dir <- file.path(export_dir, "error_report_sql")
  dir.create(sql_error_report_dir)

  log_file <- file.path(export_dir, "log-cohort-evaluation.txt")

  loggit("INFO", paste0("Log file: ", log_file), app = "evaluate_cohort_against_databases")


  if (sink_log) {
    sink(file = log_file, append = FALSE)
  }

  S3_bucket_server <- Sys.getenv("S3_bucket_server")
  S3_bucket_cdsqlite_path <- Sys.getenv("S3_bucket_cdsqlite_path")
  S3_bucket_region <- Sys.getenv("S3_bucket_region")
  run_Environment <- Sys.getenv("run_Environment")
  redshift_username <- Sys.getenv("redshift_username")
  redshift_password <- Sys.getenv("redshift_password")


  home_page_df <- get_home_page_df()

  CohortID_split <- CohortID %>% gsub("__COMMA__", ",", .) %>% strsplit(",") %>% unlist()

  datatable_rows_selected <- home_page_df %>%
    dplyr::mutate(row_id = dplyr::row_number()) %>%
    dplyr::filter(`Cohort ID` %in% CohortID_split) %>%
    dplyr::pull(row_id)

  logger::log_info(glue::glue("datatable_rows_selected: {toString(datatable_rows_selected, Inf)}"))

  phenotype_details <- get_phenotype_details()

  error_report_sql <- here::here("errorReportSql.txt")

  # for (db in databases) {
    # loggit("INFO", glue::glue("Running cohort {CohortID} evaluation against database '{db}'"), app = "evaluate_cohort_against_databases")
    logger::log_info(glue::glue("Running cohort {paste0(CohortID, collapse = ', ')} evaluation against '{paste0(databases, collapse = ', ')}'"))

    error_report_sql <- here::here("errorReportSql.txt")
    if (file.exists(error_report_sql)) {
      file.remove(error_report_sql)
    }

    results <- launch_cohort_evaluation(
      datatable_rows_selected = datatable_rows_selected,
      dataBase = databases,
      CohortID = CohortID,
      atlas_user_name = atlas_user_name,
      atlas_password = atlas_password,
      home_page_df = home_page_df,
      phenotype_details = phenotype_details,
      PL_app_dbs = PL_app_dbs,
      S3_bucket_server = S3_bucket_server,
      S3_bucket_cdsqlite_path = S3_bucket_cdsqlite_path,
      S3_bucket_region = S3_bucket_region,
      run_Environment = run_Environment,
      redshift_username = redshift_username,
      redshift_password = redshift_password,
      session_user = session_user,
      session = NULL,
      unlink_outputs = FALSE,
      export_dir = export_dir,
      force_evaluation = force_evaluation,
      postgresql_schema_name = "phenotype_library"
    )

    FUN_OUTPUT <- results

    error_report_sql <- here::here("errorReportSql.txt")
    if (file.exists(error_report_sql)) {
      file.copy(error_report_sql, file.path(
        sql_error_report_dir,
        glue::glue("{{paste0(CohortID, collapse = ', ')}}__{paste0(databases, collapse = ', ')}__errorReportSql.txt")
      ))
      file.remove(error_report_sql)
    }
  # }

    # ---------------- merging into ZIP

  if (F) { # ------- this 'if' could be possibly removed
  zipFileExportFolder <- file.path(export_dir, glue::glue("zips-{CohortID}-{format(Sys.time(), \"%Y-%m-%d-%H-%M-%S\")}"))
  # zipFileExportFolder <- "export/10616-evaluation/1734947571/zips-10616-2024-12-23-05-18-11/"

  dir.create(zipFileExportFolder, recursive = TRUE)
  logger::log_info(glue::glue("(evaluate_cohort_against_databases) Copying zip files to: '{zipFileExportFolder}'"))

  list.dirs(export_dir, full.names = TRUE) %>%
    sapply(function(dir) {
      if (basename(dir) %in% PL_app_dbs$Folder) {
        message(dir)
        list.files(dir, pattern = "\\.zip$", full.names = TRUE) %>%
          sapply(function(zfile) {
            loggit("INFO", glue::glue("Copying '{zfile}' to '{zipFileExportFolder}'"), app = "evaluate_cohort_against_databases")
            file.copy(zfile, zipFileExportFolder, overwrite = TRUE)
          })
      }
    })

  loggit("INFO", "Merging zip files", app = "evaluate_cohort_against_databases")


  # Set the path and name of the new sqlite file that will be generated by the function
  mergedSqliteDbPath <- file.path(zipFileExportFolder, glue::glue("{CohortID}.sqlite"))

  checkmate::assert_directory_exists(zipFileExportFolder)
  # Call the CD Function to generate teh SQLite file
  CohortDiagnostics::createMergedResultsFile(zipFileExportFolder,
    sqliteDbPath = mergedSqliteDbPath,
    overwrite = TRUE
  )

  # find export dir that contains all csv
  export_dir_with_csvs <- list.dirs(export_dir, full.names = TRUE) %>%
    sapply(function(dir) {
      if (basename(dir) %in% PL_app_dbs$Folder) {
        # message(dir)
        fs::path_abs(dir)
      } else {
        NA
      }
    }, USE.NAMES = FALSE) %>%
    na.omit() %>%
    as.character()

  checkmate::assert_scalar(export_dir_with_csvs)

  # check if sqlite in export dir from generate_sqlite_filepath and if yes,
  # them just copy it instead of appendin

  sqliteDbPathAfterEvaluation <- file.path(export_dir_with_csvs, glue::glue("{CohortID}.sqlite"))
  
  # Check if all required tables exist in the SQLite file
  if (file.exists(sqliteDbPathAfterEvaluation)) {
    conn_check <- DBI::dbConnect(RSQLite::SQLite(), sqliteDbPathAfterEvaluation)
    on.exit(DBI::dbDisconnect(conn_check), add = TRUE)
    
    existing_tables <- DBI::dbListTables(conn_check)
    
    # Check correlation analysis tables
    corr_tables_exist <- all(
      get_correlation_analysis_output_table_names(CohortID) %in% existing_tables
    )
    
    # Check time dependent analysis tables
    time_dep_tables_exist <- all(
      get_time_dependent_analysis_output_table_names() %in% existing_tables
    )
    
    # Check prevalence changes tables
    prevalence_tables_exist <- all(
      get_prevalence_changes_analysis_output_table_names() %in% existing_tables
    )
    
    if (corr_tables_exist && time_dep_tables_exist && prevalence_tables_exist) {
      logger::log_info(glue::glue("All analysis tables found in {sqliteDbPathAfterEvaluation}. Copying to {mergedSqliteDbPath}"))
      file.copy(sqliteDbPathAfterEvaluation, mergedSqliteDbPath, overwrite = TRUE)
    } else {
      logger::log_warn(glue::glue("Missing analysis tables in {sqliteDbPathAfterEvaluation}. Appending missing tables..."))
      
      # Append correlation analysis tables if missing
      if (!corr_tables_exist) {
        logger::log_info("Appending correlation analysis tables...")
        append_correlation_analysis_tables_to_sqlite(
          exportFolder = export_dir_with_csvs,
          sqliteDbPath = mergedSqliteDbPath,
          cohort_id = CohortID
        )
      }
      
      # Append time dependent analysis tables if missing
      if (!time_dep_tables_exist) {
        logger::log_info("Appending time dependent analysis tables...")
        append_time_dependent_analysis_tables_to_sqlite(
          exportFolder = export_dir_with_csvs,
          sqliteDbPath = mergedSqliteDbPath,
          cohort_id = CohortID
        )
      }
      
      # Append prevalence changes tables if missing
      if (!prevalence_tables_exist) {
        logger::log_info("Appending prevalence changes analysis tables...")
        append_prevalence_changes_analysis_tables_to_sqlite(
          exportFolder = export_dir_with_csvs,
          sqliteDbPath = mergedSqliteDbPath,
          cohort_id = CohortID
        )
      }
    }
  } else {
    logger::log_warn(glue::glue("SQLite file {sqliteDbPathAfterEvaluation} does not exist. Creating with all analysis tables..."))
    
    # Create SQLite file with all analysis tables
    append_correlation_analysis_tables_to_sqlite(
      exportFolder = export_dir_with_csvs,
      sqliteDbPath = mergedSqliteDbPath,
      cohort_id = CohortID
    )
    
    append_time_dependent_analysis_tables_to_sqlite(
      exportFolder = export_dir_with_csvs,
      sqliteDbPath = mergedSqliteDbPath,
      cohort_id = CohortID
    )
    
    append_prevalence_changes_analysis_tables_to_sqlite(
      exportFolder = export_dir_with_csvs,
      sqliteDbPath = mergedSqliteDbPath,
      cohort_id = CohortID
    )
  }

  if (sink_log) {
    sink()
  }

  if (!file.exists(mergedSqliteDbPath)) {
    stop("File does not exist: ", mergedSqliteDbPath)
  }

  file_size <- file.size(mergedSqliteDbPath)
  if (file_size <= 1048576) {
    stop(glue::glue("The file size of '{mergedSqliteDbPath}' is less than 1 MB. Something went wrong."))
  }


  logger::log_success("Evaluation is done")

  if (upload_to_s3) {
    logger::log_info(glue::glue("Uploading '{mergedSqliteDbPath}' to S3"))
    uploaded_s3_object <- upload_sqlite_to_s3(mergedSqliteDbPath)
    send_mail_sqlite_to_s3(uploaded_s3_object, CohortID, databases, session_user)
  }

  list(
    exportDir = export_dir,
    zipFileExportFolder = zipFileExportFolder,
    mergedSqliteDbPath = mergedSqliteDbPath
  )
  } # -------- end of merging - 'if' probably can be removed

  logger::log_success("Function evaluate_cohort_against_databases finished")
  return(FUN_OUTPUT)
}


#' upload_sqlite_to_s3
#'
#' @param sqlite_path
#' @param s3_bucket_name
#' @param s3_key_prefix
#'
#' @return
#' @export
#'
upload_sqlite_to_s3 <- function(sqlite_path, s3_bucket_name = Sys.getenv("S3_bucket_server"), s3_key_prefix = "CD_SQLITE/OPTUM_EHR") {
  checkmate::assert_file_exists(sqlite_path)

  if (!nzchar(Sys.getenv("AWS_ACCESS_KEY_ID"))) {
    stop("AWS_ACCESS_KEY_ID is not set")
  }

  if (!nzchar(Sys.getenv("AWS_DEFAULT_REGION"))) {
    stop("AWS_DEFAULT_REGION is not set")
  }

  if (!nzchar(Sys.getenv("AWS_SECRET_ACCESS_KEY"))) {
    stop("AWS_SECRET_ACCESS_KEY is not set")
  }


  s3_object <- glue::glue("{s3_bucket_name}/{s3_key_prefix}/", basename(sqlite_path), sep = "")


  loggit("INFO", glue::glue("Uploading '{s3_object}'"), app = "evaluate_cohort_against_databases")

  aws.s3::put_object(
    file = sqlite_path,
    object = s3_object,
    multipart = TRUE
  )

  s3_object
}
