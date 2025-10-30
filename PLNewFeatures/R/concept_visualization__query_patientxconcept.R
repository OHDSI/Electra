#' Generate a query to extract the patients id with a concept from omop cdm
#'
#' @description Generate a query to extract the patients id with a specific concept
#' from a domain from the omop cdm
#'
#' @param domain_table name of the domain table from the omop cdm
#' @param domain_concept_id name of the field matching to concept_id from the omop cdm
#' @param scratch scratch space where the cohort is available
#' @param cdm_schema cdm schema name from the dataset
#' @param concept_id concept id from table concepts of omop cdm
#' @param cohort_id atlas cohort id of the phenotype to be evaluated
#'
#' @author Luisa Martínez (10SEP2024)
query_patientxconcept <- function(domain_table,
                                  domain_concept_id,
                                  scratch,
                                  cdm_schema,
                                  concept_id,
                                  cohort_id) {

  checkmate::assert_string(domain_table, min.chars = 1)
  checkmate::assert_string(domain_concept_id, min.chars = 1)
  checkmate::assert_string(scratch, min.chars = 1)
  checkmate::assert_string(cdm_schema, min.chars = 1)
  checkmate::assert_numeric(concept_id)
  checkmate::assert_numeric(cohort_id)

  query <- paste0(
    "SELECT DISTINCT person_id FROM ", cdm_schema, ".", domain_table,
    " WHERE person_id IN (SELECT subject_id FROM ", scratch,
    " WHERE cohort_definition_id = '",
    cohort_id, "') AND ",
    domain_concept_id, " = '", concept_id, "'"
  )

  return(query)
}


#' query_all_patientxconcept
#'
#' Query the cdm to extract patients with a specific concept from any table
#'
#' @param main_table
#' @param source_concept_id
#' @param concept_id
query_all_patientxconcept <- function(main_table,
                                      source_concept_id,
                                      concept_id) {
  query <- paste0(
    "SELECT DISTINCT person_id FROM cdm.", main_table,
    " WHERE ", source_concept_id, " = '", concept_id, "'"
  )

  return(query)
}
