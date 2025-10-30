#' get_executed_cohorts
#'
#' @returns
#' @export
#'
#' @examples
get_executed_cohorts <- function() {
  logger::log_info("(get_executed_cohorts) Pulling executed cohorts from PostgreSQL database...")
  sql_executed_cohorts <- '
  with joined_data as (
  	select d.database_id, d.database_name, c.cohort_id, c.cohort_name from phenotype_library.cohort c left join phenotype_library.database d on c.database_id = d.database_id where c.database_id is not null order by d.database_id, c.cohort_id
  )
  select distinct database_name, cohort_id, cohort_name from joined_data'

  executed_cohorts <- run_query_RW(sql_executed_cohorts)

  executed_cohorts_df <- data.frame(executed_cohorts)
  colnames(executed_cohorts_df) <-
    c(
      "Database Name",
      "Cohort ID",
      "Cohort Name"
    )
  logger::log_success("(get_executed_cohorts) Pulled executed cohorts from PostgreSQL database...")

  executed_cohorts_df
}
