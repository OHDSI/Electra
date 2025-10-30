#' get_phenotype_details
#'
#' @return
#' @export
#'
get_phenotype_details <- function() {
  scratch_space_table <-
    "phenotype_library.phenotype_details"
  sql_phenotype_details <- paste("SELECT * FROM ", scratch_space_table)
  phenotype_details <- run_query(sql_phenotype_details)
}
