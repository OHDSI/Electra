


#' send_mail_sqlite_to_s3
#'
#' @param uploaded_s3_object
#' @param CohortID
#' @param databases
#' @param session_user
#'
#' @returns bool
#' @export
#'
send_mail_sqlite_to_s3 <- function(uploaded_s3_object, CohortID, databases, session_user) {
  checkmate::assert_string(uploaded_s3_object, min.chars = 3)
  checkmate::assert_string(session_user, min.chars = 1)
  stopifnot(
    "databases argument is missing" = !missing(databases),
    "session_user argument is missing" = !missing(session_user)
  )

  logger::log_info("(send_mail_sqlite_to_s3) Sending Email...")

  from <- sprintf(Sys.getenv("app_email"))
  to <- sprintf(paste(session_user, Sys.getenv("company_common_email"), sep = ""))
  subject <- paste(
    "SQLite from Cohort Evaluation was uploaded to S3 for Cohort(s) ",
    paste0(CohortID, collapse = ", "),
    " and database ",
    paste0(databases, collapse = ", "),
    sep = ""
  )
  body <- glue::glue("Hello,
       Great News! Your Cohort Evaluation output - SQLite file - was successfully uploaded to AWS S3 bucket. You can now access the app and visualize your results.

Thank you,
ELECTRA App
")

  sendmailR::sendmail(from, to, subject, body, control = list(smtpServer = Sys.getenv("servidor_SMTP")))
  logger::log_success("(send_mail_sqlite_to_s3) e-mail sent!")
  invisible(TRUE)
}



#' send_mail_zip_to_database
#'
#' @param CohortID
#' @param databases
#' @param session_user
#'
#' @returns
#' @export
#'
send_mail_zip_to_database <- function(CohortID, databases, session_user) {
  checkmate::assert_string(session_user, min.chars = 1)
  stopifnot(
    "databases argument is missing" = !missing(databases),
    "session_user argument is missing" = !missing(session_user)
  )

  logger::log_info("(send_mail_zip_to_database) Sending Email...")


  from <- sprintf(Sys.getenv("app_email")) # the sender’s name is an optional value
  to <- sprintf(paste(session_user, Sys.getenv("company_common_email"), sep = ""))
  subject <- paste("Your Cohort Evaluation is now Ready for Cohort(s) ", paste0(CohortID, collapse = ", "), " and database ", paste0(databases, collapse = ", "), sep = "")
  body <- paste("Hello,
       Great News! Your Cohort Evaluation is now COMPLETE. You can now access the app and visualize your results.

       If you do not see them, please select the same database(s) and cohort(s) and click 'Evaluate' and then your results will be accessible under 'Phenotype Evaluation' tab.

Thank you,
ELECTRA App
", sep = "")


  sendmailR::sendmail(from, to, subject, body, control = list(smtpServer = Sys.getenv("servidor_SMTP")))

  logger::log_success("(send_mail_zip_to_database) e-mail sent!")
  invisible(TRUE)}
