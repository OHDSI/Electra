#' get_cache_obj
#'
#' @returns
#' @export
#'
get_cache_obj <- function() {
  checkmate::assert_string(Sys.getenv("AWS_ACCESS_KEY_ID"), min.chars = 3L)
  checkmate::assert_string(Sys.getenv("AWS_SECRET_ACCESS_KEY"), min.chars = 3L)
  checkmate::assert_string(Sys.getenv("S3_bucket_server"), min.chars = 3L)
  bucket_name <- gsub("^s3://", "", Sys.getenv("S3_bucket_server"))
  memoise::cache_s3(bucket_name, compress = "gzip")
}
