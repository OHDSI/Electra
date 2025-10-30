concept_visualizer <- function(cohort_id,
                               scratch,
                               prev_change_out,
                               correlation_out,
                               orphan_out) {
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
  # Concept identification per table
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

  # Restructure tables from outputs
  ## Direct for prevalence changes
  prev_change_tbl <- prev_change_out[order(`Pc/Pt`)]
  ## Extract frequency tablas per domain for correlations
  correlation_tbl <- NULL
  for (dom in 1:length(dom_name)) {
    # Create naming per domain
    naming <- paste0(dom_name[dom], "_freq_concepts")
    # Create domain table to bind
    add <- correlation_out[[naming]][, .(
      "Concept id" = concept_id,
      "Concept Name" = concept_name,
      "Domain" = dom_atlas[dom],
      "Population (%)" = pop_perc
    )]
    correlation_tbl <- rbind(
      correlation_tbl,
      add
    )
  }
  correlation_tbl <- correlation_tbl[order(`Population (%)`)]
  ## Clean orphan codes table
  orphan_tbl <- as.data.table(orphan_out)[, .(
    "Concept id" = Id,
    "Concept Name" = Name,
    "Domain" = Class
  )]


  ui <- fluidPage(
    h1("Observe suggested concepts overlap with the cohort"),
    fluidRow(
      column(8, tabsetPanel(
        tabPanel("Prevalence Changes", DT::dataTableOutput("tbl_pc")),
        tabPanel("Correlation Analysis", DT::dataTableOutput("tbl_cr")),
        tabPanel("Orphan Concepts", DT::dataTableOutput("tbl_op"))
      ))
    ),
    fluidRow(
      column(8, plotOutput("overlap_plot"))
    ),
    fluidRow(
      column(8, plotOutput("upset_plot"))
    ),
    hr(),
  )

  server <- shinyServer(function(input, output, session) {
    output$tbl_pc <- DT::renderDataTable({
      DT::datatable(prev_change_tbl)
    })

    output$tbl_cr <- DT::renderDataTable({
      DT::datatable(correlation_tbl)
    })

    output$tbl_op <- DT::renderDataTable({
      DT::datatable(orphan_tbl)
    })

    # Query the patients from the selected rows
    options_table <- reactiveVal(NULL)
    prev_selected <- reactiveVal(NULL)

    query <- paste0(
      "SELECT DISTINCT person_id
    FROM cdm.condition_occurrence
    WHERE person_id IN (SELECT subject_id
    FROM ", scratch, " WHERE cohort_definition_id = '",
      cohort_id, "')"
    )

    patients <- dbGetQuery(conn, query)
    patients$concept <- TRUE
    colnames(patients) <- c("person_id", "Cohort")

    euler_table <- reactiveVal(patients)

    # When selecting a row from the table, retrieve patients with that code and complete euler table
    observeEvent(input$tbl_pc_rows_selected, {
      # Exclude lines previously selected
      new_line <- input$tbl_pc_rows_selected[!(input$tbl_pc_rows_selected %in% prev_selected())]
      # Include the selected rows into the previously selected reactive value
      prev_selected(input$tbl_pc_rows_selected)

      # Not retrieve the patients if the code was already included
      if (prev_change_tbl[new_line, 2] %in% colnames(euler_table())) {

      } else {
        # Determine the Domain from the concept
        dom_c <- which(dom_atlas == as.character(prev_change_tbl[new_line, 3]))

        query <- query_all_patientxconcept(
          dom_name[dom_c],
          dom_concept[dom_c],
          prev_change_tbl[new_line, 1]
        )

        patients <- dbGetQuery(conn, query)
        patients$concept <- TRUE
        colnames(patients) <- c("person_id", prev_change_tbl[new_line, 2])

        # Include extracted patients in euler_table
        total <- merge(euler_table(),
          patients,
          by = "person_id",
          all.x = TRUE,
          all.y = TRUE
        )

        total[is.na(total)] <- FALSE
        euler_table(total)
      }
    })

    table2plot <- reactive({
      plot(euler(euler_table()[, -1]),
        fills = c("snow", "red1", "grey39"),
        counts = TRUE, legend = TRUE
      )
    })

    output$overlap_plot <- renderPlot(table2plot())

    table2upset <- reactive({
      tbl2upst <- euler_table()[, -1]
      tbl2upst[tbl2upst == FALSE] <- 0
      tbl2upst[tbl2upst == TRUE] <- 1

      UpSetR::upset(tbl2upst, order.by = "freq", text.scale = 2)
    })

    output$upset_plot <- renderPlot(table2upset())
  })

  shinyApp(ui, server)
}
