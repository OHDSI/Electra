


#' get_cohort_definition_json
#'
#' @param cohort_id
#' @param folder_json
#' @param atlas_user_name
#' @param atlas_password
#' @param authenticate
#'
#' @returns
#' @export
#'
get_cohort_definition_json <- function(cohort_id,
                                       folder_json = paste(getwd(), "JSON", sep = "/"),
                                       atlas_user_name = NULL,
                                       atlas_password = NULL,
                                       authenticate = FALSE,
                                       remove_json_dir = TRUE) {
  if (remove_json_dir) {
    on.exit({
      if (dir.exists(folder_json))
        unlink(folder_json, recursive = TRUE, force = TRUE)
    }, add = TRUE)
  }

  httr::set_config(httr::config(ssl_verifypeer = 0L))

  if (authenticate) {
    # Connect with ATLAS
    ROhdsiWebApi::authorizeWebApi(
      baseUrl = Sys.getenv("Atlas_url"),
      authMethod = "windows",
      webApiUsername = atlas_user_name,
      webApiPassword = atlas_password
    )
  }

  # json_data <- jsonlite::fromJSON(json_file_path)

  #### Store the JSON file: Cohort Definition into S3 Bucket
  # bucket_contents <- aws.s3::get_bucket_df(bucket = "s3://itx-bhq-phenotype-library/CD_JSON/", region = Sys.getenv("AWS_DEFAULT_REGION")) # Update the region

  for (c_id in cohort_id) {
    json_path_on_s3 <- paste("CD_JSON/", paste0(c_id, ".json"), sep = "")

    if (aws.s3::object_exists(json_path_on_s3, Sys.getenv("S3_bucket_server"))) {
      logger::log_info(
        "The Cohort Definition:JSON file for the given cohort ID: ",
        c_id,
        " already exists in S3 bucket"
      )
    } else {
      logger::log_info(glue::glue("Pulls cohort {c_id} definition from OHDSI ATLAS"))
      cohortDefinitionSet <- ROhdsiWebApi::exportCohortDefinitionSet(
        baseUrl = Sys.getenv("Atlas_url"),
        cohortIds = c_id,
        generateStats = TRUE
      )

      logger::log_info("Download and Read the JSON file")
      CohortGenerator::saveCohortDefinitionSet(cohortDefinitionSet = cohortDefinitionSet, jsonFolder = folder_json)

      json_file_path <- paste(folder_json, "/", c_id, ".json", sep = "")
      logger::log_info("Uploading ", json_file_path, " to S3")
      aws.s3::put_object(
        file = json_file_path,
        object = paste(
          paste0(Sys.getenv("S3_bucket_server"), "/CD_JSON/"),
          c_id,
          ".json",
          sep = ""
        ),
        multipart = TRUE
      )
    }
  }

  logger::log_success("get_cohort_definition_json has completed")
  invisible(TRUE)
}
