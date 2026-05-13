#' get_cache_obj
#'
#' @returns cache object for memoise
#' @export
#'
get_cache_obj <- function() {
  # Si necesitas usar cache local en lugar de S3, puedes usar:
  # memoise::cache_filesystem() o simplemente no usar cache
  
  # Por ahora, retorna NULL para deshabilitar el cache
  NULL
}
