# Install the following packages
# remotes::install_github("OHDSI/CohortDiagnostics")
# remotes::install_github("OHDSI/Eunomia")
# remotes::install_github("OHDSI/ROhdsiWebApi")
# remotes::install.packages("dplyr")
# remotes::install_packages("OHDSI/OhdsiShinyModules")
# Sys.setenv(TZ = "UTC")
#
# install.packages("Eunomia")
# install.packages("OhdsiShinyModules")
# install.packages("CohortDiagnostics")
# install.packages("ROhdsiWebApi")
# install.packages("dplyr")
# install.packages("rjson")
# install.packages("jsonlite")
# install.packages("shiny")
# library(CohortDiagnostics)
# library(DatabaseConnector)
# library(ROhdsiWebApi)
# library(httr)
# library(jsonlite)
# library(shiny)


# # Required to run the first time to ensure you have the appropriate driver for Redshift.
# downloadJdbcDrivers(
# dbms = "redshift",
# pathToDriver = paste0(getwd(),"/drivers"),
# method = "auto"
# )
#' get_clinical_code_list
#'
#' @param atlas_user_name
#' @param atlas_password
#' @param cohortTable
#' @param CohortID
#'
#' @return
#' @export
#'
get_clinical_code_list <- function(
    atlas_user_name = NULL,
    atlas_password = NULL,
    cohortTable = NULL,
    CohortID = NULL) {

  print(paste("Start Getting Clinical Code list for cohort ID: ", CohortID))

  # Set up url
  baseUrl <- Sys.getenv("Atlas_url")

  # httr::set_config(config(ssl_verifypeer = 0L))

  ROhdsiWebApi::authorizeWebApi(
    baseUrl = baseUrl,
    authMethod = "windows",
    webApiUsername = atlas_user_name,
    webApiPassword = atlas_password
  )

  print("Export Cohort Definition set")

  cohortIds <- CohortID

  # This pulls the cohort definition from OHDSI ATLAS
  cohortDefinitionSet <- ROhdsiWebApi::exportCohortDefinitionSet(
    baseUrl = baseUrl,
    cohortIds = as.double(cohortIds),
    generateStats = TRUE
  )

  print("generate Cohort Def Set to folder")
  # print (cohortDefinitionSet)
  # print (paste(getwd(),"/inst/cohorts", sep = ""))


  CohortGenerator::saveCohortDefinitionSet(
    cohortDefinitionSet = cohortDefinitionSet,
    jsonFolder = file.path(paste(getwd(), "/inst/cohorts", sep = "")),
    sqlFolder = file.path(paste(getwd(), "/inst/sql/sql_server", sep = ""))
  )

  cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = cohortTable)


  # Specify the path to your JSON file
  json_file_path <- paste(getwd(), "/inst/cohorts/", cohortIds, ".json", sep = "")
  # print("create json_data from file path")
  # print(json_file_path)
  # Read the JSON file
  json_data <- jsonlite::fromJSON(json_file_path)
  # print("------------------------------------------------ Print json_data -------------------------------------------------")
  # print(json_data)
  #
  # print("------------------------------------------------ Print json_data$ConceptSets$expression$items -------------------------------------------------")
  # print(json_data$ConceptSets$expression$items)
  #
  # print('-------------------------------------------- Start lapply ------------------------------------------------------------------------')

  # Extract all CONCEPT_ID values using lapply
  concept_set_ids <- lapply(json_data$ConceptSets$expression$items, function(item) item$concept$CONCEPT_ID)
  concept_set_names <- lapply(json_data$ConceptSets$expression$items, function(item) item$concept$CONCEPT_NAME)
  concept_set_codes <- lapply(json_data$ConceptSets$expression$items, function(item) item$concept$CONCEPT_CODE)
  concept_set_domainIds <- lapply(json_data$ConceptSets$expression$items, function(item) item$concept$DOMAIN_ID)
  concept_set_vocabIds <- lapply(json_data$ConceptSets$expression$items, function(item) item$concept$VOCABULARY_ID)

  # print('-------------------------------------------- Start unlist ------------------------------------------------------------------------')

  # Convert the result to a vector
  concept_set_ids <- unlist(concept_set_ids)
  concept_set_names <- unlist(concept_set_names)
  concept_set_codes <- unlist(concept_set_codes)
  concept_set_domainIds <- unlist(concept_set_domainIds)
  concept_set_vocabIds <- unlist(concept_set_vocabIds)
  #
  # print('-------------------------------------------- Print Values ------------------------------------------------------------------------')

  # Print all CONCEPT_ID values
  # print(concept_set_ids)
  # print(concept_set_names)
  # print(concept_set_codes)
  # print(concept_set_domainIds)
  # print(concept_set_vocabIds)

  # print(paste("Value of length(concept_set_ids): ", length(concept_set_ids)))


  # Get the clinical code list data into the proposed table

  print("-------------------------------------------- Start Loop ------------------------------------------------------------------------")

  sql_string_start <- "INSERT INTO phenotype_library.requested_phenotype_clinical_code_list (  cohort_id,  concept_id,  concept_name,  concept_code,  domain_id,  vocabulary_id) VALUES ('"
  sql_string_end <- "');"
  sql_string_full <- ""


  for (i in seq_len(length(concept_set_ids))) {
    # print (paste("value of i: ", i))
    # print(cohortIds)
    # print(paste(concept_set_ids[[i]]))
    # print(paste(concept_set_names[[i]]))
    # print(paste(concept_set_codes[[i]]))
    # print(paste(concept_set_domainIds[[i]]))
    # print(paste(concept_set_vocabIds[[i]]))

    sql_string_full <- paste(sql_string_full, sql_string_start, paste(cohortIds, concept_set_ids[[i]], str_replace(concept_set_names[[i]], "'", "''"), concept_set_codes[[i]], concept_set_domainIds[[i]], concept_set_vocabIds[[i]], sep = "','"), sql_string_end, sep = "")
    # print("printing sql_string_full ")
    # print(sql_string_full)
    #
  }

  print("printing sql_string_full ")
  print(sql_string_full)

  # Clear the folders with temp data
  unlink(paste(getwd(), "/inst/cohorts", sep = ""), force = TRUE)
  unlink(paste(getwd(), "/inst/sql/sql_server", sep = ""), force = TRUE)
  unlink(paste(getwd(), "/inst", sep = ""), force = TRUE)

  # Returns the arguments required to update the database
  return(
    sql_string_full = sql_string_full
  )
}
