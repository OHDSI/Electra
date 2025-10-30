#' get_home_page_df
#'
#' @return data.frame
#' @export
#'
get_home_page_df <- function() {
  sql_home_page <-
    "SELECT DISTINCT
      A.PHENOTYPE_ID,
      PHENOTYPE_NAME,
       LEAD_AUTHOR,
       CO_AUTHORS,
       DOMAINS,
       CODING_SYSTEMS,
       A.COHORT_ID
FROM (SELECT DISTINCT COHORT_ID,
             PHENOTYPE_NAME,
             PHENOTYPE_ID,
             LEAD_AUTHOR,
             CO_AUTHORS,
             STRING_AGG(DOMAIN_ID,',' ) OVER (PARTITION BY COHORT_ID) AS DOMAINS
      FROM (SELECT DISTINCT A.JNJ_COHORT_DEFINITION_ID AS COHORT_ID,
                   A.PHENOTYPE_NAME,
                   A.PHENOTYPE_ID,
                   A.LEAD_AUTHOR,
                   A.CO_AUTHORS,
                   B.DOMAIN_ID
            FROM PHENOTYPE_LIBRARY.PHENOTYPE_DETAILS AS A
              INNER JOIN PHENOTYPE_LIBRARY.PHENOTYPE_CLINICAL_CODE_LIST AS B ON A.JNJ_COHORT_DEFINITION_ID = B.COHORT_ID) as D) AS A
  INNER JOIN (SELECT DISTINCT COHORT_ID,
                     STRING_AGG(VOCABULARY_ID,',' ) OVER (PARTITION BY COHORT_ID) AS CODING_SYSTEMS
              FROM (SELECT DISTINCT A.JNJ_COHORT_DEFINITION_ID AS COHORT_ID,
                           B.VOCABULARY_ID
                    FROM PHENOTYPE_LIBRARY.PHENOTYPE_DETAILS AS A
                      INNER JOIN PHENOTYPE_LIBRARY.PHENOTYPE_CLINICAL_CODE_LIST AS B ON A.JNJ_COHORT_DEFINITION_ID = B.COHORT_ID) as C) AS B ON A.COHORT_ID = B.COHORT_ID ; "

  # This table is displayed in the Home Page along with other attributes of the phenotype
  if (as.character.default(Sys.getenv("run_Environment") == "development")) {
    loggit("INFO", "Execute Home_page:", app = "global.r")
  }

  home_page <- run_query_RW(sql_home_page)

  home_page_df <- data.frame(home_page)
  colnames(home_page_df) <-
    c(
      "Phenotype ID",
      "Phenotype Name",
      "Lead Author",
      "Co Authors",
      "Domains",
      "Coding Systems",
      "Cohort ID"
    )

  home_page_df
}
