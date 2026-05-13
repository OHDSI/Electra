#' get_cohort_definition_json
#'
#' @param cohort_id
#' @param folder_json
#' @param atlas_user_name
#' @param atlas_password
#' @param authenticate
#' @param local_json_path 
#'
#' @returns
#' @export
#'
get_cohort_definition_json <- function(cohort_id,
                                       folder_json = paste(getwd(), "JSON", sep = "/"),
                                       atlas_user_name = NULL,
                                       atlas_password = NULL,
                                       authenticate = FALSE,
                                       remove_json_dir = TRUE,
                                       local_json_path = Sys.getenv("local_json_path")) {  
  
  # ... código anterior igual ...
  
  for (c_id in cohort_id) {
    
    # Guardar en local
    local_json_file <- file.path(local_json_path, paste0(c_id, ".json"))
    
    if (file.exists(local_json_file)) {
      logger::log_info(
        "The Cohort Definition:JSON file for the given cohort ID: ",
        c_id,
        " already exists locally"
      )
    } else {
      logger::log_info(glue::glue("Pulls cohort {c_id} definition from OHDSI ATLAS"))
      cohortDefinitionSet <- ROhdsiWebApi::exportCohortDefinitionSet(
        baseUrl = Sys.getenv("Atlas_url"),
        cohortIds = c_id,
        generateStats = TRUE
      )

      logger::log_info("Download and Read the JSON file")
      CohortGenerator::saveCohortDefinitionSet(
        cohortDefinitionSet = cohortDefinitionSet, 
        jsonFolder = folder_json
      )

      json_file_path <- paste(folder_json, "/", c_id, ".json", sep = "")
      
      # Crear directorio si no existe
      if (!dir.exists(local_json_path)) {
        dir.create(local_json_path, recursive = TRUE)
      }
      
      logger::log_info("Saving ", json_file_path, " locally to ", local_json_file)
      file.copy(json_file_path, local_json_file, overwrite = TRUE)
    }
  }

  logger::log_success("get_cohort_definition_json has completed")
  invisible(TRUE)
}
