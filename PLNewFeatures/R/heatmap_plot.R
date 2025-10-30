#' Plot a heatmap from the tables retrieve using correlation_analysis
#'
#' @description Plot a heatmap from the tables retrieve in a list from the
#' correlation_analysis function
#'
#' @param corr_information correlation table retrieve from the output of the function
#' correlation_analysis
#' @param table_domain name of the omop cdm domain represented in the table, this
#' name will appear in the title of the plot
#'
#' @details
#'
#' ## Required packages
#' * ggplot2
#' * plotly
#' * stringr
#'
#' @import ggplot2
heatmap_plot <- function(corr_information,
                         table_domain) {


  if (!shiny::isTruthy(corr_information$V1)) {
    stop("V1 column has no values")
  }
  if (!shiny::isTruthy(corr_information$V2)) {
    stop("V2 column has no values")
  }
  if (!shiny::isTruthy(corr_information$V3)) {
    stop("V3 column has no values")
  }

  heatmap_plotly <- plotly::plot_ly(
    data = corr_information,
    x = ~V2,
    y = ~V1,
    z = ~V3,
    type = "heatmap",
    colorscale = "Picnic",
    zmin = -1,
    zmax = 1,
    colorbar = list(title = "Correlation")
  ) %>%
    plotly::layout(
      title = paste0(
        "Correlation Heatmap between Concept Set and Frequent Codes\n",
        table_domain
      ),
      xaxis = list(title = "Frequent Codes"),
      yaxis = list(title = "Concept set Codes")
    )

  return(heatmap_plotly)
}

