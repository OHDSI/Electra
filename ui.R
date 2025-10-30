Electra_theme <- create_theme(
  adminlte_color(
    light_blue = "#4B0082",  # índigo profundo
    yellow = "#6A0DAD",      # púrpura real
    orange = "#5D3FD3"       # púrpura clásico
  ),
  adminlte_sidebar(
    width = "400px",
    dark_bg = "#4B0082",       # fondo índigo
    dark_hover_bg = "#6A0DAD", # hover púrpura real
    dark_color = "#D3D3D3"     # texto claro con tono lavanda
  ),
  adminlte_global(
    box_bg = "#FFFFFF",
    info_box_bg = "#5D3FD3"    # púrpura clásico
  )
)
# "cduiControls" function is derived from the github source "https://github.com/OHDSI/CohortDiagnostics/blob/03c40ab06832a329f2c904fbd686a0596ea9af6f/inst/shiny/DiagnosticsExplorer/ui.R"
# In case of any updates released by OHDSI regarding UI, visit the above mentioned link and replace the current code with the code specified in the git source
cdUiControls <- function(ns) {
  panels <- shiny::tagList(
    shiny::conditionalPanel(
      condition = "input.tabs != 'incidenceRate' &
                    input.tabs != 'timeDistribution' &
                    input.tabs != 'cohortCharacterization' &
                    input.tabs != 'cohortCounts' &
                    input.tabs != 'indexEventBreakdown' &
                    input.tabs != 'cohortDefinition' &
                    input.tabs != 'conceptsInDataSource' &
                    input.tabs != 'orphanConcepts' &
                    input.tabs != 'inclusionRuleStats' &
                    input.tabs != 'visitContext' &
                    input.tabs != 'compareCohortCharacterization' &
                    input.tabs != 'cohortCharacterization' &
                    input.tabs != 'cohortOverlap'",
      ns = ns,
      shinyWidgets::pickerInput(
        inputId = ns("database"),
        label = "Database",
        choices = NULL,
        multiple = FALSE,
        choicesOpt = list(style = rep_len("color: black;", 999)),
        options = shinyWidgets::pickerOptions(
          actionsBox = TRUE,
          liveSearch = TRUE,
          size = 10,
          liveSearchStyle = "contains",
          liveSearchPlaceholder = "Type here to search",
          virtualScroll = 50
        )
      )
    ),

    # Conditional panel for selecting multiple characteristics present in CD

    shiny::conditionalPanel(
      condition = "input.tabs=='incidenceRate' |
                    input.tabs == 'timeDistribution' |
                    input.tabs == 'cohortCounts' |
                    input.tabs == 'indexEventBreakdown' |
                    input.tabs == 'conceptsInDataSource' |
                    input.tabs == 'orphanConcepts' |
                    input.tabs == 'inclusionRuleStats' |
                    input.tabs == 'visitContext' |
                    input.tabs == 'cohortOverlap'",
      ns = ns,
      shinyWidgets::pickerInput(
        inputId = ns("databases"),
        label = "Database(s)",
        choices = NULL,
        multiple = TRUE,
        choicesOpt = list(style = rep_len("color: black;", 999)),
        options = shinyWidgets::pickerOptions(
          actionsBox = TRUE,
          liveSearch = TRUE,
          size = 10,
          liveSearchStyle = "contains",
          liveSearchPlaceholder = "Type here to search",
          virtualScroll = 50
        )
      )
    ),

    # Conditional panel for selecting

    shiny::conditionalPanel(
      condition = "input.tabs != 'databaseInformation' &
                    input.tabs != 'cohortDefinition' &
                    input.tabs != 'cohortCounts' &
                    input.tabs != 'cohortOverlap'&
                    input.tabs != 'incidenceRate' &
                    input.tabs != 'compareCohortCharacterization' &
                    input.tabs != 'cohortCharacterization' &
                    input.tabs != 'timeDistribution'",
      ns = ns,
      shinyWidgets::pickerInput(
        inputId = ns("targetCohort"),
        label = "Cohort",
        choices = c(""),
        multiple = FALSE,
        choicesOpt = list(style = rep_len("color: black;", 999)),
        options = shinyWidgets::pickerOptions(
          actionsBox = TRUE,
          liveSearch = TRUE,
          liveSearchStyle = "contains",
          size = 10,
          liveSearchPlaceholder = "Type here to search",
          virtualScroll = 50
        )
      )
    ),

    # Conditional panel for selecting

    shiny::conditionalPanel(
      condition = "input.tabs == 'cohortCounts' |
                    input.tabs == 'cohortOverlap' |
                    input.tabs == 'incidenceRate' |
                    input.tabs == 'timeDistribution'",
      ns = ns,
      shinyWidgets::pickerInput(
        inputId = ns("cohorts"),
        label = "Cohorts",
        choices = c(""),
        selected = c(""),
        multiple = TRUE,
        choicesOpt = list(style = rep_len("color: black;", 999)),
        options = shinyWidgets::pickerOptions(
          actionsBox = TRUE,
          liveSearch = TRUE,
          liveSearchStyle = "contains",
          size = 10,
          dropupAuto = TRUE,
          liveSearchPlaceholder = "Type here to search",
          virtualScroll = 50
        )
      )
    ),
    shiny::conditionalPanel(
      condition = "input.tabs == 'temporalCharacterization' |
                    input.tabs == 'conceptsInDataSource' |
                    input.tabs == 'orphanConcepts'",
      ns = ns,
      shinyWidgets::pickerInput(
        inputId = ns("conceptSetsSelected"),
        label = "Concept sets",
        choices = c(""),
        selected = c(""),
        multiple = TRUE,
        choicesOpt = list(style = rep_len("color: black;", 999)),
        options = shinyWidgets::pickerOptions(
          actionsBox = TRUE,
          liveSearch = TRUE,
          size = 10,
          liveSearchStyle = "contains",
          liveSearchPlaceholder = "Type here to search",
          virtualScroll = 50
        )
      )
    )
  )

  return(panels)
}

# Function to create the Cohort Diagnostics UI

#' Cohort Diagnostics UI
#' @param id        Namespace id "DiagnosticsExplorer"
#' @param enabledReports   enabled reports
cohortDiagnosticsUi <- function(id = "DiagnosticsExplorer",
                                enabledReports) {
  ns <- shiny::NS(id)
  headerContent <- tags$li(
    class = "dropdown",
    style = "margin-top: 8px !important; margin-right : 5px !important"
  )

  header <-
    shinydashboard::dashboardHeader(title = "ELECTRA", headerContent, titleWidth = 450)
  sidebarMenu <-
    shinydashboard::sidebarMenu(
      id = ns("tabs"),
      shinydashboard::menuItem(
        text = "Home",
        tabName = "home",
        icon = shiny::icon("home")
      ),
      # shinydashboard::menuItem(
      #   text = "Evaluated Phenotypes",
      #   tabName = "evaluatedPhenotypes",
      #   icon = shiny::icon("dna")
      # ),


      shinydashboard::menuItem(
        text = "Phenotype Definition",
        tabName = "phenotypeDescription",
        icon = shiny::icon("file"),
        menuSubItem("Phenotype Definition", tabName = "phenotype_Definition"),
        menuSubItem("Authors Refrences Citations", tabName = "phenotype_AuthorsRefrencesCitations"),
        menuSubItem("Clinical Description", tabName = "phenotype_ClinicalDescription"),
        menuSubItem("Logic Description", tabName = "phenotype_LogicDescription"),
        menuSubItem("Clinical Code List", tabName = "phenotype_ClinicalCodeList")
      ),
      shinydashboard::menuItem(
        text = "Phenotype Evaluation",
        tabName = "phenotypeDescription",
        icon = shiny::icon("rocket"),
        menuSubItem(
          text = "Cohort Definition",
          tabName = "cohortDefinition",
          icon = shiny::icon("code")
        ),
        menuSubItem(
          text = "Concepts in Data Source",
          tabName = "conceptsInDataSource",
          icon = shiny::icon("table")
        ),
        menuSubItem(
          text = "Orphan Concepts",
          tabName = "orphanConcepts",
          icon = shiny::icon("notes-medical")
        ),
        menuSubItem(
          text = "Concepts Correlation",
          tabName = "conceptsCorrelation",
          icon = shiny::icon("table")
        ),
        menuSubItem(
          text = "Prevalence Changes",
          tabName = "prev_changes",
          icon = shiny::icon("table")
        ),
        menuSubItem(
          text = "Time Dependent Analysis",
          tabName = "time_dep",
          icon = shiny::icon("table")
        ),
        menuSubItem(
          text = "Cohort Counts",
          tabName = "cohortCounts",
          icon = shiny::icon("bars")
        ),
        menuSubItem(
          text = "Incidence Rate",
          tabName = "incidenceRate",
          icon = shiny::icon("plus")
        ),
        if ("temporalCovariateValue" %in% enabledReports) {
          menuSubItem(
            text = "Time Distributions",
            tabName = "timeDistribution",
            icon = shiny::icon("clock")
          )
        },
        if ("indexEventBreakdown" %in% enabledReports) {
          menuSubItem(
            text = "Index Event Breakdown",
            tabName = "indexEventBreakdown",
            icon = shiny::icon("hospital")
          )
        },
        if ("visitContext" %in% enabledReports) {
          menuSubItem(
            text = "Visit Context",
            tabName = "visitContext",
            icon = shiny::icon("building")
          )
        },
        if ("relationship" %in% enabledReports) {
          menuSubItem(
            text = "Cohort Overlap",
            tabName = "cohortOverlap",
            icon = shiny::icon("circle")
          )
        },
        if ("temporalCovariateValue" %in% enabledReports) {
          menuSubItem(
            text = "Cohort Characterization",
            tabName = "cohortCharacterization",
            icon = shiny::icon("user")
          )
        },
        if ("temporalCovariateValue" %in% enabledReports) {
          menuSubItem(
            text = "Compare Characterization",
            tabName = "compareCohortCharacterization",
            icon = shiny::icon("users")
          )
        },
        menuSubItem(
          text = "Meta data",
          tabName = "databaseInformation",
          icon = shiny::icon("gear", verify_fa = FALSE)
        )
      ),
      menuItem(
        text = "Evaluate your Own Cohort(s)",
        tabName = "evaluateOwnCohort",
        icon = shiny::icon("table")
      ),

      # menuItem(
      #   text = "Create Merged Results",
      #   tabName = "createMergedResults",
      #   icon = shiny::icon("code-fork")),

      shinydashboard::menuItem(
        text = "New Phenotype Requests",
        tabName = "newphenotyperequests",
        icon = shiny::icon("list-alt")
      ),
      menuItem(
        text = "Propose a New Phenotype",
        tabName = "proposeNewPhenotype",
        icon = shiny::icon("gear")
      ),
      menuItem(
        text = "Request to withdraw an existing Phenotype",
        tabName = "requestWithdraw",
        icon = shiny::icon("table")
      ),



      # ,
      # shinydashboard::menuItem(
      #   text = "Cohort Definition",
      #   tabName = "cohortDefinition",
      #   icon = shiny::icon("code")
      # ),
      # shinydashboard::menuItem(
      #   text = "Concepts in Data Source",
      #   tabName = "conceptsInDataSource",
      #   icon = shiny::icon("table")
      # ),
      # shinydashboard::menuItem(
      #   text = "Orphan Concepts",
      #   tabName = "orphanConcepts",
      #   icon = shiny::icon("notes-medical")
      # ),
      # shinydashboard::menuItem(
      #   text = "Cohort Counts",
      #   tabName = "cohortCounts",
      #   icon = shiny::icon("bars")
      # ),
      # shinydashboard::menuItem(
      #   text = "Incidence Rate",
      #   tabName = "incidenceRate",
      #   icon = shiny::icon("plus")
      # ),
      # if ("temporalCovariateValue" %in% enabledReports) {
      #   shinydashboard::menuItem(
      #     text = "Time Distributions",
      #     tabName = "timeDistribution",
      #     icon = shiny::icon("clock")
      #   )
      # },
      # if ("indexEventBreakdown" %in% enabledReports) {
      #   shinydashboard::menuItem(
      #     text = "Index Event Breakdown",
      #     tabName = "indexEventBreakdown",
      #     icon = shiny::icon("hospital")
      #   )
      # },
      # if ("visitContext" %in% enabledReports) {
      #   shinydashboard::menuItem(
      #     text = "Visit Context",
      #     tabName = "visitContext",
      #     icon = shiny::icon("building")
      #   )
      # },
      # if ("relationship" %in% enabledReports) {
      #   shinydashboard::menuItem(
      #     text = "Cohort Overlap",
      #     tabName = "cohortOverlap",
      #     icon = shiny::icon("circle")
      #   )
      # },
      # if ("temporalCovariateValue" %in% enabledReports) {
      #   shinydashboard::menuItem(text = "Cohort Characterization",
      #                            tabName = "cohortCharacterization",
      #                            icon = shiny::icon("user"))
      # },
      # if ("temporalCovariateValue" %in% enabledReports) {
      #   shinydashboard::menuItem(
      #     text = "Compare Characterization",
      #     tabName = "compareCohortCharacterization",
      #     icon = shiny::icon("users")
      #   )
      # },
      # shinydashboard::menuItem(
      #   text = "Meta data",
      #   tabName = "databaseInformation",
      #   icon = shiny::icon("gear", verify_fa = FALSE)
      # ),
      # Conditional dropdown boxes in the side bar ------------------------------------------------------
      cdUiControls(ns)
    )

  # Side bar code
  sidebar <-
    shinydashboard::dashboardSidebar(sidebarMenu,
      width = NULL,
      collapsed = FALSE
    )

  # Body - items in tabs --------------------------------------------------

  bodyTabItems <- shinydashboard::tabItems(
    shinydashboard::tabItem(
      tabName = "home",
      h2("Evaluated Phenotypes"),
      h4("Please select a phenotype from the table below. This will display the phenotype definition and allow you to visualize the analysis results. This description aims to help users understand how to use this table."),
      h4("On the left pane, if you would like to propose a new phenotype candidate, please click the 'Propose a New Phenotype' button. When evaluating your phenotype, please fill in the request form, and we can run one or two cohort diagnostics."),
      # shiny::selectInput(
      #   "method_cd",
      #   selectize = FALSE,
      #   label = h5("Select Phenotype/Enter Cohort ID"),
      #   choices = c("Select Phenotype using table", "Evaluate your own CohortID")
      # ), br(),
      # shinyWidgets::pickerInput(
      #   inputId = "dataBase",
      #   label = h5("Select database"),
      #   choices = c(PL_app_dbs$Name),
      #   selected = PL_app_dbs$Name[1],
      #   multiple = FALSE
      # ),br(),
      DT::DTOutput("datatable"),
      #
      # shiny::conditionalPanel(condition = "input.method_cd == 'Select Phenotype using table'",
      #                         dataTableOutput("datatable")),





      # shiny::conditionalPanel(
      #   condition = "input.method_cd == 'Evaluate your own CohortID'",
      #

      #   numericInput(
      #     inputId = "CohortID_Optional",
      #     label = h5("(Optional) Second Cohort Definition ID"),
      #     value = NULL
      #
      #   ),
      #
      #   div(
      #     h4("ATLAS's Credentials"),
      #     textInput(inputId = "atlas_user_name",
      #               label = h5("Enter Username")),
      #     passwordInput(inputId = "atlas_password",
      #                   label = h5("Enter Password")),
      #   ),
      # ),

      # #add text box for user, reason, and Phenotype Picker. Then click "Send Withdraw Request"
      # shiny::fluidRow(
      #   shinydashboard::box(
      #     solidHeader = TRUE,
      #     width = 12,
      #     closable = FALSE,
      #     title = "Withdraw Request!",
      #     shiny::column(2,
      #                   shiny::textInput("userEmail", label = "User Email:")),
      #     shiny::column(
      #       4,
      #       shiny::textInput("withdrawReason", label = "Withdraw Reason:")
      #     )
      #     ,
      #     shiny::column(
      #       4,
      #       shinyWidgets::pickerInput(
      #         inputId = "phenotypeToWithdraw",
      #         label = "Select Phenotype to Withdraw",
      #         choices = c(phenotype_details$phenotype_name),
      #         selected = phenotype_details$phenotype_name[1],
      #         multiple = FALSE
      #       )
      #     )
      #     ,
      #     shiny::column(
      #       4,
      #       shiny::actionButton("sendWithdrawEmail", "Send Withdraw Request!")
      #     )
      #   )
      # ),

      div(style = "height:15px"),
    ),
    shinydashboard::tabItem(
      tabName = "newphenotyperequests",
      h2("New Phenotype Requests"),
      h4("Select a phenotype to Approve, Reject, or Request More Information "),
      span(
        style = "float:right",
        span(shiny::actionButton("approvePhenotype", "Approve", icon("plus"),
          style = "color: #fff; background-color: #59b759; border-color: #000000"
        )),
        span(style = {
          "padding-left: 15px"
        }, shiny::actionButton("rejectPhenotype", "Reject", icon("close"),
          style = "color: #fff; background-color: #EB1700; border-color: #000000"
        )),
        span(style = {
          "padding-left: 15px"
        }, shiny::actionButton("requestMoreInfoPhenotype", "Request More Information", icon("question"),
          style = "color: #fff; background-color: #Dac039; border-color: #000000"
        )), br(), br(),
      ),

      # actionButton("refreshRequests", "Refresh Table"),
      DT::DTOutput("phenotyperequestsdatatable"), width = 6, br(),
      h3("Requested Phenotype Details"),
      uiOutput("requested_phenotype_dynamic_boxes")
    ),
    shinydashboard::tabItem(
      tabName = "phenotype_Definition",
      uiOutput("dynamic_boxes")
    ),
    shinydashboard::tabItem(
      tabName = "phenotype_AuthorsRefrencesCitations",
      uiOutput("dynamic_boxes_AuthorsRefrencesCitations")
    ),
    shinydashboard::tabItem(
      tabName = "phenotype_ClinicalDescription",
      uiOutput("dynamic_boxes_clinicalDescription")
    ),
    shinydashboard::tabItem(
      tabName = "phenotype_LogicDescription",
      uiOutput("dynamic_boxes_LogicDescription")
    ),
    shinydashboard::tabItem(
      tabName = "phenotype_ClinicalCodeList",
      uiOutput("dynamic_boxes_ClinicalCodeList")
    ),
    shinydashboard::tabItem(
      tabName = "cohortDefinition",
      OhdsiShinyModules::cohortDefinitionsView(ns(
        "cohortDefinitions"
      ))
    ),
    shinydashboard::tabItem(
      tabName = "cohortCounts",
      OhdsiShinyModules::cohortCountsView(ns("cohortCounts"))
    ),
    shinydashboard::tabItem(
      tabName = "incidenceRate",
      OhdsiShinyModules::incidenceRatesView(ns("incidenceRates"))
    ),
    shinydashboard::tabItem(
      tabName = "timeDistribution",
      OhdsiShinyModules::timeDistributionsView(ns(
        "timeDistributions"
      ))
    ),
    shinydashboard::tabItem(
      tabName = "conceptsInDataSource",
      OhdsiShinyModules::conceptsInDataSourceView(ns("conceptsInDataSource"))
    ),
    shinydashboard::tabItem(
      tabName = "conceptsCorrelation",
      conceptsCorrelationUI("conceptsCorrelation")
    ),
    shinydashboard::tabItem(
      tabName = "prev_changes",
      prevalenceChangesUI("prev_changes")
    ),
    shinydashboard::tabItem(
      tabName = "time_dep",
      timeDependentUI("time_dep")
    ),
    shinydashboard::tabItem(
      tabName = "orphanConcepts",
      OhdsiShinyModules::orpahanConceptsView(ns("orphanConcepts"))
    ),
    shinydashboard::tabItem(
      tabName = "indexEventBreakdown",
      OhdsiShinyModules::indexEventBreakdownView(ns("indexEvents"))
    ),
    shinydashboard::tabItem(
      tabName = "visitContext",
      OhdsiShinyModules::visitContextView(ns("visitContext"))
    ),
    shinydashboard::tabItem(
      tabName = "cohortOverlap",
      OhdsiShinyModules::cohortOverlapView(ns("cohortOverlap"))
    ),
    shinydashboard::tabItem(
      tabName = "cohortCharacterization",
      OhdsiShinyModules::cohortDiagCharacterizationView(ns("characterization"))
    ),
    shinydashboard::tabItem(
      tabName = "compareCohortCharacterization",
      OhdsiShinyModules::compareCohortCharacterizationView(ns("compareCohortCharacterization"))
    ),
    shinydashboard::tabItem(
      tabName = "databaseInformation",
      OhdsiShinyModules::databaseInformationView(ns("databaseInformation")),
    ),
    shinydashboard::tabItem(
      tabName = "evaluateOwnCohort",
      h2("Evaluate your own Cohort(s)"),
      br(),
      fluidRow(
        # column(
        #   width = 6,
        #   shinyWidgets::pickerInput(
        #     inputId = "dataBase",
        #     label = h4("Select Database(s)"),
        #     choices = c(PL_app_dbs$Name),
        #     selected = PL_app_dbs$Name[1],
        #     multiple = TRUE,
        #     width = "100%"
        #   ),
        #
        #   shinyBS::bsTooltip(
        #     id = "CohortID",
        #     title = "This input is depreacted. Please use \"Evaluate Cohort Against Databases\" instead.",
        #     placement = "bottom"
        #   ),
        #   numericInput(
        #   inputId = "CohortID_Optional",
        #   label = h5("(Optional) Second Cohort Definition ID"),
        #   value = NULL
        #
        #   ),
        #   actionButton(
        #     inputId = "Launch",
        #     label = "Launch Evaluation",
        #     icon = icon("rocket")
        #   ),
        #   shinyBS::bsTooltip(
        #     id = "Launch",
        #     title = "This button is depreacted. Please use \"Evaluate Cohort Against Databases\" instead.",
        #     placement = "bottom"
        #   )
        # ),

      column(width = 6,
             mod_evaluate_cohort_ui("evaluate_cohort")
      ),


      # Added bordered Textbox
        column(
          width = 6,
          div(
            style = "border: 10x solid black; padding: 10x; margin-top: 10x; background-color: transparent;",
            p(
              style = "font-weight: bold; font-style: italic; color: red",
              "--------------------------------------------------***Note***--------------------------------------------------"
            ),
            # br(),
            p(
              style = "font-weight: bold; font-style: italic; color: black",
              "Please check if the database and cohort id combination exists in the below 'Executed Cohorts' table.",
              br(),
              "- If combination exists, then you can select database/s and enter cohort id/s to retrieve existing cohort data from database.",
              br(),
              "- If combination doesn't exist, then you can select database/s and enter cohort id/s to evaluate new cohorts from rHealth."
            ),
            # br(),
            p(
              style = "font-weight: bold; font-style: italic; color: red",
              "----------------------------------------------------------------------------------------------------------------"
            )
          ),
          # fluidRow(
          #   mod_evaluate_cohort_ui("evaluate_cohort")
          # )
        )
      ),
      br(),
      br(),
      h4("Executed Cohorts"),
      DT::DTOutput("executedDbCohortdatatable"), width = 6, br(),
      # ,
      #
      # div(
      #   h4("ATLAS's Credentials"),
      #   textInput(inputId = "atlas_user_name",
      #             label = h5("Enter Username")),
      #   passwordInput(inputId = "atlas_password",
      #                 label = h5("Enter Password")),
      # )
    ),
    shinydashboard::tabItem(
      tabName = "createMergedResults",
      h2("Create Your Own Merged Results"),
      h4("Please select previously evaluated Cohorts for which you would like to generate a merged file."),
      br(),
      shinyWidgets::pickerInput(
        inputId = "mergedDatabases",
        label = h5("Select Database(s)"),
        choices = c(PL_app_dbs$Name),
        selected = PL_app_dbs$Name[1],
        multiple = TRUE
      ), br(),
      shinyWidgets::pickerInput(
        inputId = "mergedCohorts",
        label = h5("Select Previously Evaluated Cohort(s)"),
        choices = c("OPTUM_EHR - 11901", "OPTUM_EHR - 13830", "OPTUM_EHR - 13211", "JMDC - 13211", "JMDC - 13830", "JMDC - 11901", "JMDC - 11980"),
        selected = NULL,
        multiple = TRUE
      ), br(),
      shiny::actionButton("submitCreateMergedResults", "Create Merged Request"),
      shiny::actionButton("launchMergedResults", "Launch Evaluation")
    ),
    shinydashboard::tabItem(
      tabName = "proposeNewPhenotype",
      h2("Propose a new Phenotype"),
      h5(" *** Please ensure that the cohort definition ID has been evaluated with results PRIOR to submitting your request. ***"),
      br(),
      h4("Phenotype Details"),
      textInput("requestnew_phenotypeName", label = "Phenotype Name:"),
      textInput("requestnew_leadAuthor", label = "Lead Author:"),
      textInput("requestnew_coauthors", label = "Co-Author(s) (comma delimited):"),
      # textInput("requestnew_tags", label = "Tags (comma delimited):"),
      textAreaInput("requestnew_tags", "Tags (comma delimited):", width = "500px", height = "100px"),
      textAreaInput("requestnew_references", "References:", width = "500px", height = "100px"),
      # textInput("requestnew_references", label = "References:"),
      # textInput("requestnew_clinicalDescription", label = "Clinical Description:"),
      textAreaInput("requestnew_clinicalDescription", "Clinical Description:", width = "800px", height = "200px"),
      h4("Logic Description"),
      # textInput("requestnew_logicDescription", label = "Logic Description:"),
      textAreaInput("requestnew_cohortEntryEvent", "Cohort Entry Event:", width = "500px", height = "75px"),
      textAreaInput("requestnew_indexEvent", "Index Event:", width = "500px", height = "75px"),
      textAreaInput("requestnew_inclusionExclusionCriteria", "Inclusion/Exclusion Criteria:", width = "500px", height = "75px"),
      textAreaInput("requestnew_cohortExit", "Cohort Exit:", width = "500px", height = "75px"),

      # textInput("requestnew_cohortEntryEvent", label = "Cohort Entry Event:"),
      # textInput("requestnew_indexEvent", label = "Index Event:"),
      # textInput("requestnew_inclusionExclusionCriteria", label = "Inclusion/Exclusion Criteria:"),
      # textInput("requestnew_cohortExit", label = "Cohort Exit:"),
      h4("Cohort Details"),
      # textInput("requestnew_databaseOfInterest", label = "Database of interest to conduct the evaluation:"),
      shinyWidgets::pickerInput(
        inputId = "requestnew_databaseOfInterest",
        label = h5("Select Database(s)"),
        choices = c(PL_app_dbs$Name),
        selected = PL_app_dbs$Name[1],
        multiple = TRUE
      ), br(),
      numericInput(inputId = "requestnew_jnjAtlasCohortDefId", label = "ATLAS instance cohort definition ID:", value = NULL),
      h5(" *** Again, please ensure that the cohort definition ID has been evaluated with results PRIOR to submitting your request. ***"),
      br(),
      shiny::actionButton("sendProposeNewPhenotypeEmail", "Send Proposal Request")
    ),
    shinydashboard::tabItem(
      tabName = "requestWithdraw",
      h2("Request to withdraw an existing Phenotype"),
      br(),

      # add text box for user, reason, and Phenotype Picker. Then click "Send Withdraw Request"
      textInput("userEmail", label = "User Email:"),
      textInput("withdrawReason", label = "Withdraw Reason:"),
      shinyWidgets::pickerInput(
        inputId = "phenotypeToWithdraw",
        label = "Select Phenotype to Withdraw",
        choices = c(phenotype_details$phenotype_name),
        selected = phenotype_details$phenotype_name[1],
        multiple = FALSE
      ),
      shiny::actionButton("sendWithdrawEmail", "Send Withdraw Request!"),
    )
  )

  # body
  body <-
    shinydashboard::dashboardBody(bodyTabItems, use_theme(Electra_theme))

  # main
  ui <- shinydashboard::dashboardPage(
    header = header,
    sidebar = sidebar,
    body = body
  )
  # returning the UI
  return(ui)
}

################################################## Define the main UI ####################################################

ui <- fluidPage(
  useShinyjs(),
  tags$script('$(document).on("shiny:sessioninitialized",function(){$.get("https://api.ipify.org", function(response) {Shiny.setInputValue("getIP", response);});})'),
  # verbatimTextOutput('ip'),

  # tags$head(tags$style(".shiny-notification {position: fixed; top: 40% ;left: 50%}")),

  # tags$head(tags$style(HTML("
  #   .progress-striped .bar {
  #     background-color: #149bdf;
  #     background-image: -webkit-gradient(linear, 0 100%, 100% 0, color-stop(0.25, rgba(255, 255, 255, 0.6)), color-stop(0.25, transparent), color-stop(0.5, transparent), color-stop(0.5, rgba(255, 255, 255, 0.6)), color-stop(0.75, rgba(255, 255, 255, 0.6)), color-stop(0.75, transparent), to(transparent));
  #     background-image: -webkit-linear-gradient(45deg, rgba(255, 255, 255, 0.6) 25%, transparent 25%, transparent 50%, rgba(255, 255, 255, 0.6) 50%, rgba(255, 255, 255, 0.6) 75%, transparent 75%, transparent);
  #     background-image: -moz-linear-gradient(45deg, rgba(255, 255, 255, 0.6) 25%, transparent 25%, transparent 50%, rgba(255, 255, 255, 0.6) 50%, rgba(255, 255, 255, 0.6) 75%, transparent 75%, transparent);
  #     background-image: -o-linear-gradient(45deg, rgba(255, 255, 255, 0.6) 25%, transparent 25%, transparent 50%, rgba(255, 255, 255, 0.6) 50%, rgba(255, 255, 255, 0.6) 75%, transparent 75%, transparent);
  #     background-image: linear-gradient(45deg, rgba(255, 255, 255, 0.6) 25%, transparent 25%, transparent 50%, rgba(255, 255, 255, 0.6) 50%, rgba(255, 255, 255, 0.6) 75%, transparent 75%, transparent);
  #     -webkit-background-size: 40px 40px;
  #        -moz-background-size: 40px 40px;
  #          -o-background-size: 40px 40px;
  #             background-size: 40px 40px;
  #   }
  # "))),

  mainPanel(
    width = 12,
    actionButton(
      inputId = "refresh",
      label = "Refresh the Entire Page",
      icon = icon("refresh")
    ),
    div(
      style = "height:40px; line-height: 40px",
      p(
        "Credit: This application utilizes open-source libraries. See ",
        a("Acknowledgments", href = "https://github.com/OHDSI/CohortDiagnostics/blob/main/DESCRIPTION", target = "_blank"),
        " for a list of authors and licenses."
      )
    ),
    progressBarTimer(top = FALSE),
    cohortDiagnosticsUi(id = "DiagnosticsExplorer", dataSource$enabledReports) # Render Phenotype Evaluator module (The UI script for the Cohort diagnostic tool)
  )
)
