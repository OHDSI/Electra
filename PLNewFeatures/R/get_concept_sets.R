#' @noRd
.CONCEPTS_SETS_THRESHOLD <- 30L


#' merge_concept_sets
#'
#' @param included_source_concept
#' @param concept
#' @param threshold
#' @param domain
#'
#' @returns
#' @export
#'
merge_concept_sets <- function(included_source_concept,
                               concept,
                               threshold = .CONCEPTS_SETS_THRESHOLD,
                               domain = "Condition",
                               rename_cols = TRUE) {

   checkmate::assert_data_frame(included_source_concept)
   checkmate::assert_data_frame(concept)
   checkmate::assert_numeric(.CONCEPTS_SETS_THRESHOLD)

   included_source_concept <- included_source_concept %>%
     dplyr::inner_join(
     concept %>% dplyr::select(concept_id, domain_id, concept_name)
   )
   if (checkmate::test_string(domain, min.chars = 1)) {
     # concept <- concept %>%
     #   dplyr::semi_join(included_source_concept , by = "concept_id") %>%
     #   dplyr::filter(domain_id %in% domain)
     included_source_concept <- included_source_concept %>%
       dplyr::filter(domain_id %in% domain)
   }

   included_source_concept <- included_source_concept %>%
     dplyr::arrange(dplyr::desc(concept_subjects)) %>%
     head(threshold)

   res <- included_source_concept %>%
     dplyr::distinct_all()

   logger::log_info(glue::glue("Found {length(unique(res$concept_id))} Concept Sets"))

   if (rename_cols) {
     res <- res %>%
       dplyr::select(Id = concept_id, Domain = domain_id, Name = concept_name)
   }
   res
 }

#' get_concept_sets
#'
#' @param included_source_concept.csv
#' @param concept.csv
#' @param threshold confirmed with Martínez Sánchez, Luisa
#'
#' @returns data.frame
#' @export
#'
 get_concept_sets <- function(included_source_concept.csv,
                              concept.csv,
                              threshold = .CONCEPTS_SETS_THRESHOLD,
                              domain = "Condition",
                              rename_cols = TRUE) {

  checkmate::assert_file_exists(included_source_concept.csv)
  checkmate::assert_file_exists(concept.csv)

  logger::log_info(glue::glue("(get_concept_sets) Combining Concept Sets from '{included_source_concept.csv}' and '{concept.csv}'"))

  included_source_concept <- readr::read_csv(included_source_concept.csv, show_col_types = FALSE)
  concept <- readr::read_csv(concept.csv, show_col_types = FALSE)

  merge_concept_sets(included_source_concept, concept, threshold = threshold, domain = domain, rename_cols = rename_cols)
}


#' get_concept_sets_from_db
#'
#' @param sqlite_path
#' @param threshold
#'
#' @returns
#' @export
#'
 get_concept_sets_from_db <- function(sqlite_path,
                                      threshold = .CONCEPTS_SETS_THRESHOLD,
                                      domain = "Condition",
                                      rename_cols = TRUE) {
   checkmate::assert_file_exists(sqlite_path, extension = "sqlite")

  logger::log_info("(get_concept_sets_from_db) Pulling  from ", sqlite_path)
  con <- DBI::dbConnect(RSQLite::SQLite(), sqlite_path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  logger::log_info(glue::glue("(get_concept_sets_from_db) Combining Concept Sets from {sqlite_path}"))

  included_source_concept <- DBI::dbGetQuery(con, "SELECT * FROM included_source_concept;")
  concept <- DBI::dbGetQuery(con, "SELECT * FROM concept;")

  merge_concept_sets(included_source_concept, concept, threshold = threshold, domain = domain, rename_cols = rename_cols)
}
