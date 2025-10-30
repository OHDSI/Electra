if (!require("rsconnect")) {
  install.packages("rsconnect")
}
library(dplyr)
# pkgload::load_all()
# pkgload::load_all("PLNewFeatures")

appFiles <-
  c(
    "ui.R",
    "server.R",
    "global.R",
    "DESCRIPTION",
    "NAMESPACE",
    "R",
    "renv.lock",
    "inst",
    "PL_App_Databases.csv",
    intersect(
      file.path("PLNewFeatures", list.files("PLNewFeatures", recursive = TRUE)),
      system("git ls-files", intern = TRUE)
    ),
    "drivers"
  ) %>%
  sapply(function(x) if (file.exists(x)) x else NA, USE.NAMES = FALSE) %>%
  na.omit()

rsconnect::deployApp(
  ".",
  appName = "test_electra",
  account = rsconnect::accounts()$name,
  server = rsconnect::accounts()$server,
  appFiles = appFiles
)

