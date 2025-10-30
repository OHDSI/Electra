#' Create a table with concept frequency in the general population
#'
#' @description Create a table with concept_id and frequency of the general
#' population for specific given concepts.
#'
#' @param domain_table name of the domain table from the omop cdm
#' @param domain_concept_id name of the field matching to concept_id from the omop cdm
#' @param input_concepts concepts ids from table concepts of omop cdm to retrieved
#' @param cdm_schema cdm schema name from the dataset
#'
#' @author Luisa Martínez (15SEP2024)
table_freq_spcf_concepts <- function(domain_table,
                                     domain_concept_id,
                                     input_concepts,
                                     cdm_schema) {

  query <- paste0("SELECT ", domain_concept_id, " AS concept_id , ",
                  "COUNT(DISTINCT h.person_id) AS total_pop, 100*(CAST(COUNT(DISTINCT h.person_id) AS DOUBLE)/ ",
                  "(SELECT COUNT(DISTINCT person_id) FROM ", cdm_schema, ".person)) AS total_pop_perc ",
                  "FROM ", cdm_schema, ".", domain_table, " AS h ",
                  "WHERE ", domain_concept_id, " IN (", input_concepts ,") ",
                  "GROUP BY ", domain_concept_id)

  return(query)

}
