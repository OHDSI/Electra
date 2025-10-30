#' timeDependentUI
#'
#' @param id
#'
#' @return
#' @export
#'
timeDependentUI <- function(id) {
  ns <- NS(id)
  tagList(
    fluidRow(
      column(plotly::plotlyOutput(ns("table_pats")), width = 4),
      column(plotly::plotlyOutput(ns("table_rec")), width = 4),
      column(plotly::plotlyOutput(ns("table_eofu")), width = 4)
    ),
    fluidRow(
      uiOutput(ns("plot_grid"))
    )
  )
}

#' timeDependentServer
#'
#' @param id
#' @param list_data
#'
#' @return
#' @export
#' @import shiny
#'
timeDependentServer <- function(
    id,
    cohort_id,
    selected_dbs_df,
    sqlite_db_path,
    path_to_driver = "drivers",
    threshold = 30L,
    n_plots_in_row = 4L,
    small_plot_with = 3L
) {

  moduleServer(id, function(input, output, session) {
    checkmate::assert_class(cohort_id, "reactive")
    checkmate::assert_class(selected_dbs_df, "reactive")
    checkmate::assert_class(sqlite_db_path, "reactive")
    checkmate::assert_directory_exists(path_to_driver)
    checkmate::assert_set_equal((small_plot_with * n_plots_in_row) %% 12, 0)

    ns <- session$ns

    cdm_schema <- reactive(get_cdm_schema(req(selected_dbs_df()$db_code)))

    scratch <- reactive({
      selected_dbs <- req(selected_dbs_df())
      cohortDatabaseSchema <- req(selected_dbs$Scratch_Space_Name)
      cohortTable <- get_cohort_table_for_evaluation()
      scratch <- glue::glue("{cohortDatabaseSchema}.{cohortTable}")
      # scratch <- "scratch_mriver_optum_panther.cohort_test" # <--- TEMP TO BE REMOVED
    })

    initial_tables <- reactive({
      cohort_id <- req(cohort_id()) %>% as.numeric()
      selected_dbs <- req(selected_dbs_df())
      database_id <- selected_dbs$db_code
      scratch <- req(scratch())

      progress <-  shiny::Progress$new(session, min = 0, max = 100)
      on.exit(progress$close(), add = TRUE)

      progress$set(message = 'Time Dependent Analysis', detail = 'Checking data availability', value = 25)

      # First, try to read time dependent analysis data from PostgreSQL
      tryCatch({
        time_dep_data <- read_all_time_dependent_tables(
          cohort_id = cohort_id,
          database_id = database_id
        )

        # Check if we have data for all three main tables
        has_eofup_data <- nrow(time_dep_data$patient_eofup) > 0
        has_recruitment_data <- nrow(time_dep_data$patient_recruitment) > 0
        has_patient_x_time_data <- nrow(time_dep_data$patient_x_time) > 0

        if (has_eofup_data && has_recruitment_data && has_patient_x_time_data) {
          # All data is available in PostgreSQL
          out <- list(
            table_eofu = time_dep_data$patient_eofup,
            table_rec = time_dep_data$patient_recruitment,
            table_pats = time_dep_data$patient_x_time
          )

          progress$set(message = 'Time Dependent Analysis', detail = 'Data loaded from PostgreSQL', value = 100)
          progress$close()

          logger::log_info("Successfully loaded time dependent analysis data from PostgreSQL for cohort_id: ",
                          cohort_id, " and database_id: ", database_id)

          return(out)
        } else {
          # Some or all data is missing, need to calculate and upload
          logger::log_info("Missing time dependent analysis data in PostgreSQL. Calculating and uploading...")
          progress$set(message = 'Time Dependent Analysis', detail = 'Calculating missing data', value = 50)

          # Check if scratch table exists
          conn <- get_databricks_conn(drivers_dir = path_to_driver)
          on.exit(DBI::dbDisconnect(conn), add = TRUE)

          # req(assert_scratch_space_and_show_modal(conn, scratch))

          if (!table_exists_in_databricks(conn, scratch)) {
            err_msg <- glue::glue("Scratch space table '{scratch}' does not exist. Cannot calculate time dependent analysis. Please run forced cohort evaluation.")
            logger::log_error(err_msg)
            stop(err_msg)
          }

          if (db_table_has_zero_rows(conn, scratch)) {
            err_msg <- glue::glue("Scratch space table '{scratch}' has 0 rows. Cannot calculate time dependent analysis. Please run forced cohort evaluation.")
            logger::log_error(err_msg)
            stop(err_msg)
          }

          # Get PostgreSQL connection for uploading
          postgres_conn <- DatabaseConnector::connect(get_postgresql_connection_details())
          on.exit(DBI::dbDisconnect(postgres_conn), add = TRUE)

          # Calculate missing data
          out <- list()

          if (!has_eofup_data) {
            progress$set(message = 'Time Dependent Analysis', detail = 'Calculating patient_eofup', value = 60)
            logger::log_info("Calculating patient_eofup data...")
            table_eofu <- patient_eofup(
              cohort_id = cohort_id,
              scratch.table = scratch,
              conn = conn,
              postgres_conn = postgres_conn,
              postgres_table_name = "patient_eofup",
              database_id = database_id,
              schema_name = "phenotype_library"
            )
            out$table_eofu <- table_eofu
          } else {
            out$table_eofu <- time_dep_data$patient_eofup
          }

          if (!has_recruitment_data) {
            progress$set(message = 'Time Dependent Analysis', detail = 'Calculating patient_recruitment', value = 70)
            logger::log_info("Calculating patient_recruitment data...")
            table_rec <- patient_recruitment(
              cohort_id = cohort_id,
              scratch.table = scratch,
              conn = conn,
              postgres_conn = postgres_conn,
              postgres_table_name = "patient_recruitment",
              database_id = database_id,
              schema_name = "phenotype_library"
            )
            out$table_rec <- table_rec
          } else {
            out$table_rec <- time_dep_data$patient_recruitment
          }

          if (!has_patient_x_time_data) {
            progress$set(message = 'Time Dependent Analysis', detail = 'Calculating patient_x_time', value = 80)
            logger::log_info("Calculating patient_x_time data...")
            table_pats <- patient_x_time(
              cohort_id = cohort_id,
              scratch.table = scratch,
              conn = conn,
              postgres_conn = postgres_conn,
              postgres_table_name = "patient_x_time",
              database_id = database_id,
              schema_name = "phenotype_library"
            )
            out$table_pats <- table_pats
          } else {
            out$table_pats <- time_dep_data$patient_x_time
          }

          progress$set(message = 'Time Dependent Analysis', detail = 'Data calculated and uploaded', value = 100)
          progress$close()

          logger::log_success("Successfully calculated and uploaded missing time dependent analysis data for cohort_id: ",
                             cohort_id, " and database_id: ", database_id)

          return(out)
        }

      }, error = function(e) {
        progress$close()
        err_msg <- glue::glue("Failed to load or calculate time dependent analysis data. {e$message}")
        logger::log_error(err_msg)
        showModal(errorModal(err_msg))
        req(FALSE)
      })
    })

    concept <- eventReactive(sqlite_db_path(), {
      path <- req(sqlite_db_path())
      logger::log_info("Pulling concept sets from ", path)
      con <- DBI::dbConnect(RSQLite::SQLite(), path)
      on.exit(DBI::dbDisconnect(con), add = TRUE)
      query <- "SELECT * FROM concept;"
      tryCatch({
        df <- DBI::dbGetQuery(con, query)
      }, error = function(e) {
        logger::log_error(glue::glue("Query '{query}' with error: {e$message}"))
        showModal(errorModal(paste0("Failed to pull data from 'included_source_concept'. Error: ", e$message)))
      })
    })

    output$table_pats <- plotly::renderPlotly({
      tables <- req(initial_tables())
      plotly_counts(tables$table_pats, "year_month", "total_count", tit = "Cohort count")
    })
    output$table_rec <- plotly::renderPlotly({
      tables <- req(initial_tables())
      plotly_counts(tables$table_rec, "year_month", "total_count", tit = "Patient recruitment")
    })
    output$table_eofu <- plotly::renderPlotly({
      tables <- req(initial_tables())
      plotly_counts(tables$table_eofu, "year_month", "total_count", tit = "End of follow up")
    })

    included_source_concept <- reactive({
      path <- req(sqlite_db_path())

      logger::log_info("(timeDependentServer) Pulling  from ", path)

      tryCatch({
        df <- get_concept_sets_from_db(path, rename_cols = FALSE)
      }, error = function(e) {
        print(e$message)
        showModal(errorModal(paste0("Failed to pull data from 'included_source_concept'. Error: ", e$message)))
      })
    })

    # m_code_x_time <- memoise::memoise(code_x_time, cache = get_cache_obj(), omit_args  = c("conn"))
    # m_plotly_counts <- memoise::memoise(plotly_counts, cache = get_cache_obj())

    plots <- eventReactive(list(concept(), included_source_concept(), cohort_id(), selected_dbs_df(), cdm_schema()), {
      included_source_concept <- req(included_source_concept())
      concept <- req(concept())
      cohort_id <- req(cohort_id()) %>% as.numeric()
      selected_dbs <- req(selected_dbs_df())
      database_id <- selected_dbs$db_code
      cdm_schema <- req(cdm_schema())
      scratch <- req(scratch())

      logger::log_info("(timeDependentServer) Loading code_x_time plots from PostgreSQL with fallback")

      progress <-  shiny::Progress$new(session)

      postgres_conn <- DatabaseConnector::connect(get_postgresql_connection_details())
      on.exit(DBI::dbDisconnect(postgres_conn), add = TRUE)
      concept_ids <- included_source_concept$concept_id %>%
        unique() %>%
        paste0(collapse = ", ")
      res <- DBI::dbGetQuery(postgres_conn,
                             glue::glue("SELECT * FROM phenotype_library.code_x_time
                                        WHERE cohort_id = {cohort_id}
                                        AND database_id = '{database_id}'
                                        AND concept_id IN ({concept_ids});"))

      if (nrow(res) == 0) {
        err_msg <- glue::glue("No data found in database. Cannot calculate analysis. Please run forced cohort evaluation.")
        logger::log_error(err_msg)
        showModal(errorModal(err_msg))
        req(FALSE)
      }

      plots <- included_source_concept %>%
        dplyr::select(concept_id, concept_name, domain_id) %>%
        dplyr::distinct_all() %>% {
          df <- .
          df %>% dplyr::mutate(row_id = dplyr::row_number(), rows_total = nrow(df))
        } %>%
        purrr::pmap(function(concept_id, concept_name, domain_id, row_id, rows_total) {
          logger::log_info(glue::glue("(timeDependentServer) Loading plot for Concept ID '{concept_id}' and Domain ID '{domain_id}'"))
          progress$set(message = 'Loading plot', detail = glue::glue("({row_id}/{rows_total}) Loading plot for Concept ID '{concept_id}'"), value = row_id / rows_total)

          # First, try to read code_x_time data from PostgreSQL
          tryCatch({
            table_code <- read_time_dependent_table(
              table_name = "code_x_time",
              cohort_id = cohort_id,
              database_id = database_id,
              concept_id = concept_id
            )

            if (nrow(table_code) > 0) {
              # Data found in PostgreSQL
              plotly_counts(table_code, time = "year_month", counts = "total_count", tit = concept_name)
            } else {
              # No data in PostgreSQL, try to calculate it
              logger::log_info("No code_x_time data found in PostgreSQL for concept_id: ", concept_id, ". Calculating...")

              tryCatch({
                # Get connections for calculation
                conn <- get_databricks_conn(drivers_dir = path_to_driver)
                on.exit(DBI::dbDisconnect(conn), add = TRUE)

                postgres_conn <- DatabaseConnector::connect(get_postgresql_connection_details())
                on.exit(DBI::dbDisconnect(postgres_conn), add = TRUE)

                # Calculate code_x_time data
                table_code <- code_x_time(
                  concept_id = concept_id,
                  cdm_schema = cdm_schema,
                  domain = domain_id,
                  conn = conn,
                  postgres_conn = postgres_conn,
                  postgres_table_name = "code_x_time",
                  cohort_id = cohort_id,
                  database_id = database_id,
                  schema_name = "phenotype_library"
                )

                if (!is.null(table_code) && nrow(table_code) > 0) {
                  logger::log_success("Successfully calculated and uploaded code_x_time data for concept_id: ", concept_id)
                  plotly_counts(table_code, time = "year_month", counts = "total_count", tit = concept_name)
                } else {
                  # Still no data after calculation
                  empty_data <- data.frame(year_month = as.Date(character()), total_count = numeric())
                  plotly_counts(empty_data, time = "year_month", counts = "total_count", tit = paste(concept_name, "(No Data)"))
                }
              }, error = function(calc_e) {
                logger::log_error("Error calculating code_x_time data for concept_id ", concept_id, ": ", calc_e$message)
                # Return empty plot on calculation error
                empty_data <- data.frame(year_month = as.Date(character()), total_count = numeric())
                plotly_counts(empty_data, time = "year_month", counts = "total_count", tit = paste(concept_name, "(Error)"))
              })
            }
          }, error = function(e) {
            logger::log_error("Error loading code_x_time data for concept_id ", concept_id, ": ", e$message)
            # Return empty plot on error
            empty_data <- data.frame(year_month = as.Date(character()), total_count = numeric())
            plotly_counts(empty_data, time = "year_month", counts = "total_count", tit = paste(concept_name, "(Error)"))
          })
        })
      progress$close()
      plots
    })

    output$plot_grid <- renderUI({
      plots <- req(plots())

      split_plots <- split(
        plots,
        ceiling(seq_along(plots) / n_plots_in_row)
      )

      logger::log_info(glue::glue("(timeDependentServer) Rendering smaller plots grid"))

      shiny::tagList(
        shiny::tags$hr(),
        shiny::tags$h2("Codes appearance", align = "center"),
        shiny::tags$hr(),
        lapply(split_plots, function(chunk) {
          tagList(
            lapply(chunk, function(plt) {
              id <- paste0("plot_", sample(1e6, 1))  # Unique ID
              output[[id]] <- plotly::renderPlotly(plt)
              column(width = small_plot_with, plotly::plotlyOutput(ns(id)))
            })
          )
        })
      )
    })

    return(list(NULL))
  })
}


#' assert_scratch_space_and_show_modal
#'
#' @param conn
#' @param scratch
#'
#' @returns
#' @export
#'
assert_scratch_space_and_show_modal <- function(conn, scratch) {
  if (!table_exists_in_databricks(conn, scratch)) {
    err_msg <- glue::glue("Scratch space table '{scratch}' does not exist. Cannot analysis. Please run forced cohort evaluation.")
    logger::log_error(err_msg)
    showModal(errorModal(err_msg))
    req(FALSE)
  }

  if (db_table_has_zero_rows(conn, scratch)) {
    err_msg <- glue::glue("Scratch space table '{scratch}' has 0 rows. Cannot calculate analysis. Please run forced cohort evaluation.")
    logger::log_error(err_msg)
    showModal(errorModal(err_msg))
    req(FALSE)
  }
  FALSE
}

#' errorModal
#'
#' @param message
#'
#' @returns
#' @export
#'
errorModal <- function(message) {
  modalDialog(
    title = "Error occured",
    shiny::tags$span(message),
    footer = tagList(
      modalButton("OK"),
    )
  )
}
