#' prevalenceChangesUI
#'
#' @param id
#'
#' @return
#' @export
#'
prevalenceChangesUI <- function(id) {
  ns <- NS(id)
  tagList(
    DT::DTOutput(ns("table"))
  )
}

#' prevalenceChangesServer
#'
#' @param id
#' @param list_data
#'
#' @return
#' @export
#' @import shiny
#'
prevalenceChangesServer <- function(id, cohort_id, selected_dbs_df, sqlite_db_path, path_to_driver = "drivers") {

  moduleServer(id, function(input, output, session) {
    checkmate::assert_class(cohort_id, "reactive")
    checkmate::assert_class(selected_dbs_df, "reactive")
    checkmate::assert_class(sqlite_db_path, "reactive")
    checkmate::assert_directory_exists(path_to_driver)
    ns <- session$ns

    errorModal <- function(message) {
      modalDialog(
        title = "Error occured",
        shiny::tags$span(message),
        footer = tagList(
          modalButton("OK"),
        )
      )
    }

    concept_sets <- eventReactive(sqlite_db_path(), {
      path <- req(sqlite_db_path())
      logger::log_info("Pulling concept sets from ", path)
      con <- DBI::dbConnect(RSQLite::SQLite(), path)
      on.exit(DBI::dbDisconnect(con), add = TRUE)
      tryCatch({
        df <- DBI::dbGetQuery(con, "SELECT * FROM included_source_concept;")
      }, error = function(e) {
        print(e$message)
        showModal(errorModal(paste0("Failed to pull data from 'included_source_concept'. Error: ", e$message)))
      })
    })

    m_prevalence_changes <- memoise::memoise(prevalence_changes ,cache = get_cache_obj(), omit_args  = c("conn", "progress"))


    table_to_print <- eventReactive(list(cohort_id(), selected_dbs_df(), concept_sets()), {
      cohort_id <- req(cohort_id()) %>% as.numeric()
      selected_dbs <- req(selected_dbs_df())
      database_id <- selected_dbs$db_code
      concept_sets <- req(concept_sets())
      checkmate::assert_data_frame(selected_dbs, min.rows = 1)
      checkmate::assert_names(names(selected_dbs), must.include = c("Scratch_Space_Name", "db_code"))

      progress <-  shiny::Progress$new(session, min = 0, max = 100)
      on.exit(progress$close(), add = TRUE)

      progress$set(message = 'Prevalence Changes', detail = 'Checking data availability', value = 25)

      if (nrow(selected_dbs) > 1)
        stop("Multiple databases are not supported yet.")

      # First, try to read prevalence changes data from PostgreSQL
      tryCatch({
        prevalence_data <- read_prevalence_changes_formatted(
          cohort_id = cohort_id,
          database_id = database_id
        )

        if (nrow(prevalence_data) > 0) {
          # Data is available in PostgreSQL
          progress$set(message = 'Prevalence Changes', detail = 'Data loaded from PostgreSQL', value = 100)
          progress$close()

          logger::log_info("Successfully loaded prevalence changes data from PostgreSQL for cohort_id: ",
                          cohort_id, " and database_id: ", database_id)

          return(prevalence_data)
        } else {
          err_msg <- "No prevalence changes data found in database. Run forced cohort evaluation"
          logger::log_error(err_msg)
          stop(err_msg)

          # No data in PostgreSQL, need to calculate and upload
          logger::log_error("No prevalence changes data found in database Calculating and uploading...")
          progress$set(message = 'Prevalence Changes', detail = 'Calculating missing data', value = 50)

          req(assert_scratch_space_and_show_modal(conn, scratch))


          # Get database connections and parameters
          cdm_schema <- get_cdm_schema(selected_dbs$db_code)
          server <- selected_dbs$rHealth_ServerNam
          cohortDatabaseSchema <- selected_dbs$Scratch_Space_Name
          cohortTable <- get_cohort_table_for_evaluation()
          scratch <- glue::glue("{cohortDatabaseSchema}.{cohortTable}")

          # Check if scratch table exists
          conn <- get_redshift_conn(server = server, drivers_dir = path_to_driver)
          on.exit(DBI::dbDisconnect(conn), add = TRUE)

          # if (!gsub("^.*\\.", "", scratch) %in% DBI::dbListTables(conn)) {
          #   err_msg <- glue::glue("Scratch space table '{scratch}' does not exist. Cannot calculate prevalence changes.")
          #   logger::log_error(err_msg)
          #   showModal(errorModal(err_msg))
          #   req(FALSE)
          # }

          # Get PostgreSQL connection for uploading
          postgres_conn <- DatabaseConnector::connect(get_postgresql_connection_details())
          on.exit(DBI::dbDisconnect(postgres_conn), add = TRUE)

          # Calculate prevalence changes data
          progress$set(message = 'Prevalence Changes', detail = 'Calculating prevalence changes', value = 75)
          logger::log_info("Calculating prevalence changes data...")

          out <- prevalence_changes(
            cohort_id = cohort_id,
            scratch = scratch,
            cdm_schema = cdm_schema,
            concept_sets = concept_sets,
            conn = conn,
            postgres_conn = postgres_conn,
            postgres_table_name = "prevalence_changes",
            database_id = database_id,
            schema_name = "phenotype_library"
          )

          progress$set(message = 'Prevalence Changes', detail = 'Data calculated and uploaded', value = 100)
          progress$close()

          logger::log_success("Successfully calculated and uploaded prevalence changes data for cohort_id: ",
                             cohort_id, " and database_id: ", database_id)

          return(out)
        }

      }, error = function(e) {
        progress$close()
        err_msg <- glue::glue("Failed to load or calculate prevalence changes data. Error: {e$message}")
        logger::log_error(err_msg)
        showModal(errorModal(err_msg))
        req(FALSE)
      })
    })

    # Generate UI for all list elements
    output$table <- DT::renderDT({
      df <- req(table_to_print())
      logger::log_info("Rendering table...")
      df
    })

    return(list(
      table_to_print = table_to_print
    ))
  })
}
