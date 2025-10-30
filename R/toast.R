#' Show notification with \code{shinyWidgets::show_toast}
#'
#' Wrapper of \code{shinyWidgets::show_toast}
#'
#' @param ... arguments for \code{shinyWidgets::show_toast}
#' @param timer
#'
#' @returns
#' @export
#'
toast <- function(..., timer = 10 * 1000L) {
  shinyWidgets::show_toast(..., timer = timer)
}
