#' conceptsCorrelationUI
#'
#' @param id
#'
#' @return
#' @export
#'
conceptsCorrelationUI <- function(id) {
  ns <- NS(id)

  tagList(
    uiOutput(ns("elements_ui"))
  )
}

#' conceptsCorrelationServer
#'
#' @param id
#' @param list_data
#'
#' @return
#' @export
#'
conceptsCorrelationServer <- function(id, list_data) {
  heatmap_names <- c(
    "condition_occurrence_correlations",
    "drug_exposure_correlations",
    "procedure_occurrence_correlations",
    "device_exposure_correlations",
    "measurement_correlations",
    "observation_correlations"
  )

  get_heatmap_name <- function(nm) {
    switch(gsub("_[0-9]+$", "", nm),
      condition_occurrence_correlations = "Condition occurence",
      drug_exposure_correlations = "Drug exposure",
      procedure_occurrence_correlations = "Procedure occurrence",
      device_exposure_correlations = "Device exposure",
      measurement_correlations = "Measurement",
      observation_correlations = "Observation correlation"
    )
  }

  moduleServer(id, function(input, output, session) {
    ns <- session$ns


    # Helper function to format names
    formatName <- function(name) {
      # Split the name by underscore
      words <- strsplit(name, "_")[[1]]
      # Capitalize first letter of each word
      words <- sapply(words, function(word) {
        paste0(toupper(substr(word, 1, 1)), substr(word, 2, nchar(word)))
      })
      # Join the words without underscores
      paste(words, collapse = "")
    }


    tables_exist <- reactive({
      req(list_data()) %>% sapply(shiny::isTruthy)
    })

    # Generate UI for all list elements
    output$elements_ui <- renderUI({
      list_data <- req(list_data())

      if (all(tables_exist())) {
        loggit("INFO", "Rendering UI", app = "conceptsCorrelationServer")
        elements <- lapply(names(list_data), function(name) {
          formatted_name <- formatName(name)

          tagList(
            h2(formatted_name),
            if (!is.null(get_heatmap_name(name))) plotly::plotlyOutput(ns(paste0("heatmap_", name))),
            DT::DTOutput(ns(paste0("table_", name))),
            hr()
          )
        })

        do.call(tagList, elements)
      } else {
        missing_tables <- tables_exist()[!shiny::isTruthy(tables_exist())] %>% names()
        paste0(missing_tables, collapse = ", ")
        msg <- glue::glue("Tables {paste0(missing_tables, collapse = \", \")} are missing in database. Please rerun Cohort Evaluation and try again.")
        loggit("WARN", msg, app = "conceptsCorrelationServer")

        fluidRow(
          "Following tables are missing in database:\n",
          shiny::tags$br(),
          shiny::tags$strong(paste0(missing_tables, collapse = ", ")),
          shiny::tags$br(),
          "Please rerun Cohort Evaluation and try again."
        )
      }
    })


    observeEvent(list_data(), {
      list_data <- req(list_data())
      req(all(tables_exist()))
      loggit("INFO", "Rendering Server", app = "conceptsCorrelationServer")
      # Create render functions for each list element

      for (name in names(list_data)) {
        local({
          local_name <- name
          force(local_name)
          heatmap_name <- get_heatmap_name(local_name)

          # if (!shiny::isTruthy(is.null(heatmap_name))) {
          #   logger::log_warn("Skipping rendering for ", local_name)
          #   next
          # }

          output[[paste0("heatmap_", local_name)]] <- plotly::renderPlotly({
            if (!is.null(heatmap_name)) {
              heatmap_plot(list_data[[local_name]], heatmap_name)
            } else {
              warning("Heatmap name not found for table: ", local_name)
            }
          })

          output[[paste0("table_", local_name)]] <- DT::renderDT({
            datatable(list_data[[local_name]])
          })
        })
      }
    })
  })
}
