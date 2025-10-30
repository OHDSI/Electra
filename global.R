Sys.setenv(TZ = "Europe/Warsaw")
options(java.parameters = "-Xss15m")


library(devtools)
library(shiny)
library(shinybusy)
library(shinydashboard)
library(shinyjs)
library(shinyWidgets)
library(DT)
library(RSQLite)
library(DBI)
library(httr)
library(jsonlite)
library(readxl)
library(dplyr)
library(openxlsx)
library(purrr)
library(glue)
library(aws.signature)
library(aws.s3)
library(rclipboard)
library(tidyr)
library(ps)
library(lubridate)
library(Andromeda)
library(lifecycle)
library(cli)
library(withr)
library(utf8)
library(fansi)
library(cpp11)
library(progress)
library(askpass)
library(stringr)
library(checkmate)
library(labeling)
library(vroom)
library(Rcpp)
library(timechange)
library(openssl)
library(scales)
library(RJSONIO)
library(readr)
library(easycsv)
library(profvis)
library(rJava)
library(DatabaseConnector)
library(fresh)
library(reactable)
library(tidyverse)
library(R6)
library(shinyBS)


library(SqlRender)
library(ROhdsiWebApi)
library(ResultModelManager)
library(CohortDiagnostics)
library(CohortGenerator)
library(FeatureExtraction)
library(CirceR)
library(OhdsiShinyModules)
library(loggit)
library(sendmailR)
library(httr)
library(jsonlite)
library(rjson)

logger::log_layout(logger::layout_glue_colors)
logger::log_level(logger::TRACE)

devtools::load_all("PLNewFeatures")

devtools::load_all()

set_credentials()

# check working environment. If Dev, show logs
if (as.character.default(Sys.getenv("run_Environment") == "development")) {
  set_logfile("loggit.json")
  loggit("INFO", "app has started", app = "global.r")
  loggit("INFO", "Environment:", app = "global.r")
  loggit("INFO", as.character.default(Sys.getenv("run_Environment")), app = "global.r")
}

isUserAuthenticated <- FALSE
isUserAdmin <- "NO"
# JDBC Driver: Update the path of the driver, if needed
DatabaseConnector::downloadJdbcDrivers(
  dbms = "redshift",
  pathToDriver = paste(getwd(), "drivers", sep = "/"),
  method = "auto",
  autoAgree = TRUE
)

DatabaseConnector::downloadJdbcDrivers("spark",
                                       pathToDriver = paste(getwd(), "drivers", sep = "/"))


# Connect to using function so that we can close connection after query is executed
if (as.character.default(Sys.getenv("run_Environment") == "development")) {
  loggit("INFO", "run query:", app = "global.r")
}

# Add Function for ShinyApp Progress Bar
progressBarTimer <- function(top = TRUE) {
  progressBar <- div(
    class = "progress progress-striped active",
    # disable Bootstrap's transitions so we can use jQuery.animate
    div(class = "progress-bar", style = "-webkit-transition: none !important;
              transition: none !important;")
  )

  containerClass <- "progress-timer-container"

  if (top) {
    progressBar <- div(class = "shiny-progress", progressBar)
    containerClass <- paste(containerClass, "shiny-progress-container")
  }

  tagList(
    tags$head(
      tags$script(HTML("
        $(function() {
          Shiny.addCustomMessageHandler('progress-timer-start', function(message) {
            var $progress = $('.progress-timer-container');
            var $bar = $progress.find('.progress-bar');
            $bar.css('width', '0%');
            $progress.show();
            $bar.animate({ width: '100%' }, {
              duration: message.duration,
              easing: message.easing,
              complete: function() {
                if (message.autoClose) $progress.fadeOut();
              }
            });
          });

          Shiny.addCustomMessageHandler('progress-timer-close', function(message) {
            var $progress = $('.progress-timer-container');
            $progress.fadeOut();
          });
        });
      "))
    ),
    div(class = containerClass, style = "display: none;", progressBar)
  )
}

startProgressTimer <- function(durationMsecs = 2000, easing = c("swing", "linear"),
                               autoClose = FALSE, session = getDefaultReactiveDomain()) {
  easing <- match.arg(easing)
  session$sendCustomMessage("progress-timer-start", list(
    duration = durationMsecs,
    easing = easing,
    autoClose = autoClose
  ))
}

closeProgressTimer <- function(session = getDefaultReactiveDomain()) {
  session$sendCustomMessage("progress-timer-close", list())
}

# End Progress Bar Func

# Retrieve the Phenotype details from the scratch space/central repository
phenotype_details <- get_phenotype_details()

# Retrieve all databases for the dropdown picker
PL_app_dbs <- read.csv("PL_App_Databases.csv")

# Retrieve all requests for phenotype addition
scratch_space_phenotype_library <- "phenotype_library"
table_requested_phenotype_details <- "requested_phenotype_details"
table_requested_phenotype_response <- "requested_phenotype_response"
table_requested_phenotype_clinical_code_list <- "requested_phenotype_clinical_code_list"



if (as.character.default(Sys.getenv("run_Environment") == "development")) {
  loggit("INFO", "PL_app_dbs:", app = "global.r")
  # loggit("INFO", as.character(PL_app_dbs), app="global.r")
}



# Table name: Clinical Code List
clinical_code_lists <-
  "phenotype_library.phenotype_clinical_code_list"



# home_page_df <- get_home_page_df()

# Sourcing the script containing a function that returns arguments that are required to run the server part of Cohort Diagnostics App [cohortDiagnosticsServer()]
# source(paste(getwd(), "Get_Clinical_Code_List_Function.R", sep = "/"))
# A default cohort id which is present in the S3 bucket is given here
# because the CD App can be triggered and displayed in the interface initially only when an SQLite file is passed
sqlite_path(CohortID = 11901)
