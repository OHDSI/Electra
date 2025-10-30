#' set_credentials
#'
#' @export
#'
set_credentials <- function(verbose = TRUE, path = NULL) {
  loggit::loggit("INFO", "Setting credentials", app = "set_credentials.R")

  creds_yaml <- get_credentials_path()

  creds <- yaml::read_yaml(creds_yaml)

  lapply(names(creds), function(key) {
    val <- creds[[key]]
    if (verbose) {
      message("Setting env variable ", key)
    }
    arg <- list()
    arg[[key]] <- val
    do.call(Sys.setenv, arg)
  })
  TRUE
}


#' get_credentials_path
#'
#' @returns path
#' @export
#'
get_credentials_path <- function() {
  paths <- c(
    here::here("inst", "credentials.yaml"),
    system.file("credentials.yaml", package = "PhenotypeLibrary")
  )

  existing_path <- head(purrr::keep(paths, \(p) file.exists(p)), 1)

  if (length(existing_path) == 0) {
    stop("credentials.yaml not found")
  } else {
    logger::log_success(glue::glue("Found credentials in: {existing_path}"))
    existing_path
  }
}
