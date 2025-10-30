#' plot_counts
#'
#' @author Luisa Martínez (23MAY2025)
#' @export
#' @family time_dependent_analysis
#' @import ggplot2
plot_counts <- function(table,
                        time,
                        counts,
                        date_label = "Date",
                        count_label = "Count",
                        tit = "") {

  ggplot(table ,
         aes(x=get(time) , y=get(counts), group = 1)) +
    geom_point() +
    geom_line() +
    xlab(date_label) +
    ylab(count_label) +
    ggtitle(tit) +
    scale_y_continuous(labels = scales::comma) + # Formato de números completos
    theme_minimal()

}

#' plotly_counts
#'
#' @author Luisa Martínez (23MAY2025)
#' @export
#' @family time_dependent_analysis
#' @importFrom dplyr %>%
plotly_counts <- function(table,
                          time,
                          counts,
                          date_label = "Date",
                          count_label = "Count",
                          tit = "",
                          title_font_size = 14) {

  checkmate::assert_data_frame(table)

  wrapped_title <- stringr::str_wrap(tit, width = 40) %>%
    stringr::str_replace_all("\n", "<br>")

  table %>%
    dplyr::arrange(!!rlang::sym(time)) %>%
    plotly::plot_ly(
      x = ~get(time),
      y = ~get(counts),
      type = 'scatter',
      mode = 'lines+markers',
      line = list(width = 2),
      marker = list(size = 4)
    ) %>%
    plotly::layout(
      title = list(
        text = wrapped_title,
        font = list(
          size = title_font_size
        ),
        x = 0.5, xanchor = "center"
      ),
      margin = list(t = 80),  # Increase top margin for multiple lines
      xaxis = list(
        title = date_label,
        type = 'date'  # Explicitly set x-axis type to date
      ),
      yaxis = list(
        title = count_label,
        tickformat = ","  # Format numbers with comma separators
      )
    )
}



