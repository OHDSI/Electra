#' mod_evaluate_cohort_ui
#'
#' @param id
#'
#' @return
#' @export
#'
#' @import shiny
#' @rdname mod_evaluate_cohort
mod_evaluate_cohort_ui <- function(id) {
  ns <- NS(id)
  tagList(
    # actionButton(ns("btn_evaluate"), "Evaluate Cohort Against Databases"),


    # showModal(modalDialog(
    #   title = "Evaluate Cohort Against Databases",
    #   tagList(
    #     checkboxGroupInput(ns("databases"), "Select Databases", choices = databases_df()$Name),
    #     textInput(ns("CohortID"), "Enter Cohort ID", placeholder = "Enter comma-separated Cohort IDs"),
    #     shiny::tags$hr(),
    #     fluidRow("If checked, ignores already evaluated cohorts and triggers full cohort evaluation process"),
    #     shiny::checkboxInput(ns("force_evaluation"), "Force cohort evaluation", FALSE)
    #   ),
    #   footer = tagList(
    #     modalButton("Cancel"),
    #     actionButton(ns("evaluate"), "Evaluate", icon = icon("rocket"))
    #   ),
    #   easyClose = TRUE
    # ))

    checkboxGroupInput(ns("databases"), "Select Databases", choices = NULL),
    textInput(ns("CohortID"), "Enter Cohort ID", placeholder = "Enter comma-separated Cohort IDs"),
    shiny::tags$hr(),

    fluidRow("If checked, ignores already evaluated cohorts and triggers full cohort evaluation process"),
    shiny::checkboxInput(ns("force_evaluation"), "Force cohort evaluation", FALSE),

    shiny::tags$hr(),

    actionButton(ns("evaluate"), "Evaluate", icon = icon("rocket")),

    uiOutput(ns("status"))
  )
}


#' mod_evaluate_cohort_server
#'
#' @param id
#' @param databases_csv
#'
#' @return
#' @export
#'
#' @import shiny
#'
#' @rdname mod_evaluate_cohort
mod_evaluate_cohort_server <- function(id,
                                       databases_csv,
                                       session_user,
                                       atlas_password,
                                       atlas_user_name,
                                       upload_to_s3 = FALSE) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns

    mod_output <- reactiveVal()

    if (interactive()) {
      future::plan(future::sequential)
    } else {
      future::plan(list(
        future::tweak(future::sequential), # Outer layer
        future::tweak(future::multisession, workers = future::availableCores() %/% 4) # Inner mapping
      ))
    }


    # Read database list from CSV file
    databases_df <- reactive({
      read.csv(here::here(databases_csv))
    })


    observeEvent(databases_df(), {
      req(databases_df())
      shiny::updateCheckboxGroupInput(session, inputId = "databases", choices = databases_df()$Name)
    })

    cohort_id <- reactive({
      CohortID <- gsub("__COMMA__", ",", input$CohortID)
      logger::log_info(glue::glue("(mod_evaluate_cohort_server) CohortID is: {CohortID}"))
      CohortID
    })

    observeEvent(input$btn_evaluate, {
      showModal(modalDialog(
        title = "Evaluate Cohort Against Databases",
        tagList(
          checkboxGroupInput(ns("databases"), "Select Databases", choices = databases_df()$Name),
          textInput(ns("CohortID"), "Enter Cohort ID", placeholder = "Enter comma-separated Cohort IDs"),
          shiny::tags$hr(),
          fluidRow("If checked, ignores already evaluated cohorts and triggers full cohort evaluation process"),
          shiny::checkboxInput(ns("force_evaluation"), "Force cohort evaluation", FALSE)
        ),
        footer = tagList(
          modalButton("Cancel"),
          actionButton(ns("evaluate"), "Evaluate")
        ),
        easyClose = TRUE
      ))
    })

    # max_cores <-  floor(future::availableCores() / 2)
    # max_cores <- 3L
    # logger::log_info(glue::glue("Using {max_cores} cores"))
    # future::plan(future::multisession, workers = 2L)

    eval_task <- shiny::ExtendedTask$new(function(CohortID,
                                                  databases,
                                                  session_user,
                                                  atlas_password,
                                                  atlas_user_name,
                                                  databases_csv,
                                                  upload_to_s3,
                                                  force_evaluation) {
      # prom <- promises::future_promise({
      future::future({
        tryCatch(
          {
            # devtools::load_all()
            # devtools::load_all("PLNewFeatures")
            set_credentials()

            results <- evaluate_cohort_against_databases(
              CohortID = CohortID,
              databases = databases,
              session_user = atlas_user_name,
              atlas_password = atlas_password,
              atlas_user_name = atlas_user_name,
              databases_csv = databases_csv,
              upload_to_s3 = upload_to_s3,
              sink_log = FALSE,
              force_evaluation = force_evaluation
            )

            return(results)
          },
          error = function(e) {
            logger::log_error("(mod_evaluate_cohort_server) Cohort evaluation failed. Error: ", toString(e$message, Inf))
            e$message
          }
        )
      })
    })

    bslib::bind_task_button(eval_task, "evaluate")

    observeEvent(input$evaluate, {
      # Ensure at least one database is selected and a cohort ID is provided
      if (is.null(input$databases) || isTRUE(cohort_id() == "")) {
        showNotification("Please select at least one database and provide a Cohort ID.", type = "error")
        toast("Please select at least one database and provide a Cohort ID.", type = "warn")
        return()
      }

      cohort_ids_split <- cohort_id() %>% strsplit(",") %>% unlist() %>% toString(Inf)
      shinybusy::show_modal_spinner(
        spin = "circle",
        text = shiny::HTML(glue::glue(
          "Cohort(s):<br><strong>{cohort_ids_split}</strong><br>evaluation against databases: <br><strong>{toString(input$databases, width = Inf)}</strong><br>in progress..."
          ))
      )

      logger::log_info("(mod_evaluate_cohort_server) Force evaluation: ", input$force_evaluation)

      CohortID <- cohort_id()

      if (grepl("__COMMA__", CohortID)) {
        logger::log_warn(glue::glue("(mod_evaluate_cohort_server) Removing '__COMMA__' from CohortID: {CohortID}"))
        CohortID <- gsub("__COMMA__", ",", CohortID)
      }

      # Backend function execution

       eval_task$invoke(
        CohortID,
        input$databases,
        session_user,
        req(atlas_password()),
        req(atlas_user_name()),
        databases_csv,
        upload_to_s3 = upload_to_s3,
        force_evaluation = input$force_evaluation
      )


      # Close the modal after evaluation
      removeModal()
      remove_modal_spinner()

      # Notify user of success
      showNotification("Cohort evaluation initiated successfully.", type = "message")
      toast("Cohort evaluation initiated successfully.", type = "info")

    })


    observe({
      invalidateLater(10 * 1000)
      req(input$evaluate > 0)

      output$status <- renderUI({
        if (eval_task$status() == "running") {
          shiny::tags$span(icon("spinner"), " Running")
        } else if (eval_task$status() == "success") {
          shiny::tags$span(icon("check"), " Success")
        } else if (eval_task$status() == "error") {
          shiny::tags$span(
            icon("triangle-exclamation"),
            paste0(" Error: ", as.character(eval_task$result()))
          )
        }
      })
    })


    observe({
      res <- eval_task$result()
      logger::log_success("Extended task completed for: ", toString(purrr::map_vec(res, function(x) x$cohort_id)))
      mod_output(res)
    })


    mod_output
  })
}

# library(shiny)
#
# ui <- fluidPage(
#   mod_evaluate_cohort_ui("evaluate_cohort")
# )
#
# server <- function(input, output, session) {
#   mod_evaluate_cohort_server("evaluate_cohort", databases_csv = "PL_App_Databases.csv")
# }
#
# shinyApp(ui, server)
