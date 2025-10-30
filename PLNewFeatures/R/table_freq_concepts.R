#' Generate a query to create a table with most frequent concepts per cohort/domain
#'
#' @description Generate a query to create a table with the most frequent concepts
#' from a specific cohort in a specific domain from the scratch space .
#'
#' @param domain_table name of the domain table from the omop cdm
#' @param domain_concept_id name of the field matching to concept_id from the omop cdm
#' @param scratch scratch space where the cohort is available
#' @param cdm_schema cdm schema name from the dataset
#' @param cohort_id atlas cohort id of the phenotype to be evaluated
#' @param cut_off minimum proportion of patients from the cohort having a code to
#' include the code into the output table
#'
#' @author Luisa Martínez (10SEP2024)
table_freq_concepts <- function(domain_table,
                                domain_concept_id,
                                scratch,
                                cdm_schema,
                                cohort_id,
                                cut_off = 5) {
  query <- paste0(
    "SELECT a.concept_name, b.concept_id, b.pop, b.pop_perc ",
    "FROM (SELECT ", domain_concept_id, " AS concept_id, ",
    "COUNT(DISTINCT h.person_id) AS pop, ",
    "100*( CAST( COUNT(DISTINCT h.person_id) AS DOUBLE)/ ",
    "(SELECT COUNT(DISTINCT subject_id) FROM ",
    scratch, " WHERE cohort_definition_id = ",
    cohort_id, ")) AS pop_perc ",
    "FROM ", cdm_schema, ".", domain_table, " AS h ",
    "WHERE person_id IN (SELECT subject_id FROM ", scratch, " ",
    "WHERE cohort_definition_id = ", cohort_id,
    ") GROUP BY concept_id ORDER BY pop DESC) AS b ",
    "JOIN ", cdm_schema, ".concept AS a ON b.concept_id = a.concept_id ",
    "WHERE pop_perc > ", cut_off, " ORDER BY pop_perc DESC"
  )

  return(query)
}
