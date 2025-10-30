# Source the Global.R file to access the connection details function
# source("/repos/phenotype_library_odysseus/global.R")

# Define server
server <- shinyServer(function(input, output, session) {
  # Set users IP address for storing into Session Table
  output$ip <- reactive(input$getIP)

  users_ip <- reactive(input$getIP)

  export_dir <- file.path(getwd(), "export")

  selected_dbs_df <- reactiveVal()

  # The below query creates a table with unique domain_id and vocabulary_id for each phenotype

  home_page_df <- reactive({

    logger::log_info("(server.R) Pulling home page...")

    sql_home_page <- "SELECT DISTINCT
  A.PHENOTYPE_ID,
  PHENOTYPE_NAME,
  LEAD_AUTHOR,
  CO_AUTHORS,
  DOMAINS,
  CODING_SYSTEMS,
  A.COHORT_ID
  FROM (SELECT DISTINCT COHORT_ID,
        PHENOTYPE_NAME,
        PHENOTYPE_ID,
        LEAD_AUTHOR,
        CO_AUTHORS,
        STRING_AGG(DOMAIN_ID,',' ) OVER (PARTITION BY COHORT_ID) AS DOMAINS
        FROM (SELECT DISTINCT A.JNJ_COHORT_DEFINITION_ID AS COHORT_ID,
              A.PHENOTYPE_NAME,
              A.PHENOTYPE_ID,
              A.LEAD_AUTHOR,
              A.CO_AUTHORS,
              B.DOMAIN_ID
              FROM PHENOTYPE_LIBRARY.PHENOTYPE_DETAILS AS A
              INNER JOIN PHENOTYPE_LIBRARY.PHENOTYPE_CLINICAL_CODE_LIST AS B ON A.JNJ_COHORT_DEFINITION_ID = B.COHORT_ID) as D) AS A
  INNER JOIN (SELECT DISTINCT COHORT_ID,
              STRING_AGG(VOCABULARY_ID,',' ) OVER (PARTITION BY COHORT_ID) AS CODING_SYSTEMS
              FROM (SELECT DISTINCT A.JNJ_COHORT_DEFINITION_ID AS COHORT_ID,
                    B.VOCABULARY_ID
                    FROM PHENOTYPE_LIBRARY.PHENOTYPE_DETAILS AS A
                    INNER JOIN PHENOTYPE_LIBRARY.PHENOTYPE_CLINICAL_CODE_LIST AS B ON A.JNJ_COHORT_DEFINITION_ID = B.COHORT_ID) as C) AS B ON A.COHORT_ID = B.COHORT_ID ;"

    # This table is displayed in the Home Page along with other attributes of the phenotype
    if (as.character.default(Sys.getenv("run_Environment") == "development")) {
      loggit("INFO", "Execute Home_page:", app = "server.r")
    }

    home_page <- run_query_RW(sql_home_page)

    if (as.character.default(Sys.getenv("run_Environment") == "development")) {
      loggit("INFO", "End Home_page:", app = "server.r")
    }

    home_page_df <- data.frame(home_page)
    colnames(home_page_df) <-
      c(
        "Phenotype ID",
        "Phenotype Name",
        "Lead Author",
        "Co Authors",
        "Domains",
        "Coding Systems",
        "Cohort ID"
      )

    home_page_df
  })


  # Render data table for databases and cohorts already ran
  output$executedDbCohortdatatable <- DT::renderDT({
    get_executed_cohorts()
  },
    server = FALSE
  )

  # Render data table for home page
  output$datatable <- DT::renderDT(
    home_page_df(),
    server = FALSE,
    selection = list(mode = "single")
  )
  # Get selected phenotype information
  selected_phenotype <- reactive({
    home_page_df()[input$datatable_rows_selected, ]$`Phenotype Name`
  })
  selected_row <- reactive({
    subset(phenotype_details, phenotype_name == selected_phenotype())
  })



  # Ask user to enter credentials if not authenticated
  # Check first if user has been authenticated within the last 30 minsr. If so, set isUserAuthenticated <- true. If not, get credentials.

  if (!isUserAuthenticated) {
    print("User needs to enter credentials")
    showModal(modalDialog(
      tags$h2("Please enter your ATLAS Credentials"),
      textInput(
        inputId = "atlas_user_name",
        label = h5("Enter Username"), value = NULL
      ),
      passwordInput(
        inputId = "atlas_password",
        label = h5("Enter Password"), value = NULL
      ),
      footer = tagList(
        actionButton("submitCredentials", "Submit"),
        actionButton("cancelLogin", "Cancel"),
      )
    ))
  }

  # A button to refresh the entire App
  observeEvent(input$cancelLogin, {
    stopApp()
  })
  # A button to refresh the entire App
  observeEvent(input$submitCredentials, {
    # Authenticate User. If Authenticated, save user session details to allow timeout after 30 minutes

    tryCatch(
      {
        logger::log_trace("TRY")
        # Use Atlas credentials to authenticate into app
        baseUrl <- Sys.getenv("Atlas_url")
        httr::set_config(config(ssl_verifypeer = 0L))
        ROhdsiWebApi::authorizeWebApi(
          baseUrl = baseUrl,
          authMethod = "windows",
          webApiUsername = input$atlas_user_name,
          webApiPassword = input$atlas_password
        )

        isUserAuthenticated <- TRUE
        removeModal()
        session$user <- input$atlas_user_name

        # Print Session Details


        admin_user_list <- Sys.getenv("admin_user_list") # "BPeter1;MRivera90"

        print("value of admin_user_list")
        print(Sys.getenv("admin_user_list"))


        admin_users <- unlist(strsplit(admin_user_list, ";"))

        # if user is not in the admin list, hide the requests of new phenotype tab item
        if (tolower(session$user) %in% tolower(admin_users)) {
          print("User is Admin.... show phenotype request actions")
          shinyjs::show(id = "approvePhenotype")
          shinyjs::show(id = "rejectPhenotype")
          shinyjs::show(id = "requestMoreInfoPhenotype")
          isUserAdmin <- "YES"
        } else {
          # isUserAdmin <- "NO"
          print("User is General User..... hide phenotype request actions")
          shinyjs::hide(id = "approvePhenotype")
          shinyjs::hide(id = "rejectPhenotype")
          shinyjs::hide(id = "requestMoreInfoPhenotype")
        }
      },
      error = function(msg) {
        print("Error with Authentication")
        print(conditionMessage(msg))

        # Collect Comments from User and save to Database
        showModal(modalDialog(
          tags$h2("Please enter your ATLAS Credentials"),
          textInput(
            inputId = "atlas_user_name",
            label = h5("Enter Username")
          ),
          passwordInput(
            inputId = "atlas_password",
            label = h5("Enter Password")
          ),

          # tags$head(tags$style('h4 {color:red;}')),
          h4(paste(conditionMessage(msg), "Please Re-enter the correct credentials.")),
          footer = tagList(
            actionButton("submitCredentials", "Submit"),
            actionButton("cancelLogin", "Cancel"),
          )
        ))
      },
      finally = {
        print("Authenticated!")
      }
    )



    # Get Users details to save to database
    print(paste("Value of IP: ", users_ip()))
    print(paste("Value of session$user: ", session$user))
    print(paste("Value of session$clientdata: ", session$clientdata))
    print(paste("Value of User Email: ", paste(session$user, Sys.getenv("company_common_email"), sep = "")))
    print(paste("Value of login date time: ", Sys.time()))
    print(paste("Value of time zone: ", Sys.timezone()))
    print(paste("Value of Session Token: ", session$token))
    print(paste("Value of isUserAdmin: ", isUserAdmin))



    # Run insert user session data to the db
    sql_insert_user_session <-
      paste(paste("INSERT INTO phenotype_library.user_sessions (user_name, user_email, user_ip, session_token, login_datetime, is_admin) VALUES('", tolower(session$user), "','", paste(tolower(session$user), Sys.getenv("company_common_email"), sep = ""), "','", users_ip(), "','", session$token, "','", Sys.time(), "','", isUserAdmin, "' );", sep = ""),
        sep = ""
      )

    print("Save Response Requested Phenotype to Database")
    print(sql_insert_user_session)

    execute_command(sql_insert_user_session)
  })



  # A button to refresh the entire App
  observeEvent(input$refresh, {
    refresh()
  })

  # selected_phenotype <- reactiveValues()
  # selected_row <- reactiveValues()

  phenotypeToWithdraw <- reactiveValues()

  observeEvent(input$sendProposeNewPhenotypeEmail, {
    shinyjs::disable("sendProposeNewPhenotypeEmail")
    shinyjs::show("Request Sent!")
    sql_string_full <- ""
    # Get the clinical code list data into the proposed table
    logger::log_info("(server.R) start get clinical code list")

    tryCatch(
      {
        logger::log_info("(server.R) TRY Get Clinical Code List")

        sql_string_full <- get_clinical_code_list(
          atlas_user_name = input$atlas_user_name,
          atlas_password = input$atlas_password,
          cohortTable = "cohort",
          CohortID = paste(input$requestnew_jnjAtlasCohortDefId)
        )

        logger::log_info(paste("(server.R) Value of sql_string_full: ", sql_string_full[1]))
        logger::log_success("(server.R) continue with clinical code list")




        proposedDbs <- paste(sort(input$requestnew_databaseOfInterest), collapse = " | ")





        # Run insert query to add request to the db table
        save_request_phenotype_data <-
          paste(
            paste(
              "INSERT INTO PHENOTYPE_LIBRARY.requested_phenotype_details (phenotype_name,lead_author,  co_authors,  tags,  reference_links,  clinical_description,  cohort_entry_event,  index_event,  ie_criteria,  cohort_exit,  db_interest,  jnj_cohort_definition_id, request_by_user,  request_date,  request_time) VALUES ( '",
              paste(
                gsub("'", "''", input$requestnew_phenotypeName),
                gsub("'", "''", input$requestnew_leadAuthor),
                gsub("'", "''", input$requestnew_coauthors),
                gsub("'", "''", input$requestnew_tags),
                gsub("'", "''", input$requestnew_references),
                gsub("'", "''", input$requestnew_clinicalDescription),
                gsub("'", "''", input$requestnew_cohortEntryEvent),
                gsub("'", "''", input$requestnew_indexEvent),
                gsub("'", "''", input$requestnew_inclusionExclusionCriteria),
                gsub("'", "''", input$requestnew_cohortExit),
                proposedDbs,
                input$requestnew_jnjAtlasCohortDefId,
                paste(session$user, Sys.getenv("company_common_email"), sep = ""),
                Sys.Date(),
                Sys.time(),
                sep = "','"
              ),
              "'); ",
              sep = ""
            ),
            paste(
              "INSERT INTO PHENOTYPE_LIBRARY.requested_phenotype_response(  phenotyperequest_id,  status) VALUES((select max(PHENOTYPEREQUEST_ID) from PHENOTYPE_LIBRARY.requested_phenotype_details ), 'PENDING');"
            ),
            sql_string_full[1],
            sep = ""
          )

        logger::log_success(
          "------------------------  Save Requested Phenotype to Database ---------------------------------------"
        )




        logger::log_info("(server.R) save_request_phenotype_data: ", toString(save_request_phenotype_data))

        qry <- paste(
          "INSERT INTO PHENOTYPE_LIBRARY.requested_phenotype_response(  phenotyperequest_id,  status) VALUES((select max(PHENOTYPEREQUEST_ID) from PHENOTYPE_LIBRARY.requested_phenotype_details ), 'PENDING');"
        )


        request_in_db <- run_query_RW(glue::glue("SELECT * from PHENOTYPE_LIBRARY.requested_phenotype_details where phenotype_name='{input$requestnew_phenotypeName}'"))
        if (nrow(request_in_db) > 0) {
          toast(glue::glue("Request for '{input$requestnew_phenotypeName}' already exists. Cannot proceed."), type = "warn")
          return(NULL)
        }

        execute_command(save_request_phenotype_data)



        logger::log_info("(server.R) Composing new phenotype request Email...")


        from <-
          sprintf(paste(paste(session$user, Sys.getenv("company_common_email"), sep = ""))) # the sender’s name is an optional value
        to <-
          Sys.getenv("withdraw_request_to_email") %>%
          gsub("\\s", "", .) %>%
          strsplit(split = ";") %>%
          unlist()

        cc <- sprintf(paste(session$user, Sys.getenv("company_common_email"), sep = ""))
        subject <-
          paste("Propose New Phenotype Intake Request: ",
            input$requestnew_phenotypeName,
            sep = ""
          )
        body <- paste(
          "Hello,
           Please see below details to propose a new Phenotype.

    Phenotype Name: ",
          input$requestnew_phenotypeName,
          "
    Lead Author: ",
          input$requestnew_leadAuthor,
          "
    Co-Author(s): ",
          input$requestnew_coauthors,
          "
    Tags: ",
          input$requestnew_tags,
          "
    References: ",
          input$requestnew_references,
          "
    Clinical Description: ",
          input$requestnew_clinicalDescription,
          "

    Logic Description Details
    Cohort Entry Event: ",
          input$requestnew_cohortEntryEvent,
          "
    Index Event: ",
          input$requestnew_indexEvent,
          "
    Inclusion/Exclusion Criteria: ",
          input$requestnew_inclusionExclusionCriteria,
          "
    Cohort Exit: ",
          input$requestnew_cohortExit,
          "

    Cohort Details
    Database of interest to conduct the evaluation: ",
          proposedDbs,
          "

    ATLAS instance cohort definition ID: ",
          input$requestnew_jnjAtlasCohortDefId,
          "

    Thank you,
    ",
          session$user,
          sep = ""
        )

        res <- sendmailR::sendmail(
          from,
          to,
          subject,
          body,
          cc,
          control = list(smtpServer = Sys.getenv("servidor_SMTP"))
        )

        logger::log_success(glue::glue("Email sent. Code: {res$code}. Message: {res$msg}"))
        toast(paste("E-mail sent to ", toString(to, Inf)), type = "success")
      },
      error = function(msg) {
        logger::log_error("Error with getting Code List: ", conditionMessage(msg))

        showModal(
          modalDialog(
            title = "Error with getting clinical code list! ",
            paste("Please Check the Error and try again.
            Error: ", msg),
            easyClose = TRUE,
            footer = NULL,
          )
        )

        # enable button to try again

        shinyjs::enable("sendProposeNewPhenotypeEmail")
      },
      finally = {
        logger::log_info("(server.R) clinical code list received")
      }
    )

    # refresh app
    toast("App will refresh in 5 seconds...", type = "info")
    Sys.sleep(5L)
    refresh()
  })

  observeEvent(input$sendWithdrawEmail, {
    shinyjs::disable("sendWithdrawEmail")
    shinyjs::show("Request Sent!")

    logger::log_info("(server.R) Composing Email...")

    from <- sprintf(paste(input$userEmail)) # the sender’s name is an optional value
    to <- unlist(strsplit(Sys.getenv("withdraw_request_to_email"), ";"))
    cc <- sprintf(paste(input$userEmail))
    subject <- paste("Phenotype Withdraw Request: ", input$phenotypeToWithdraw, sep = "")
    body <- paste("Hello,
       Please initiate a withdraw for the following Phenotype.

Phenotype Name: ", input$phenotypeToWithdraw, "
Reason for Withdraw: ", input$withdrawReason, "

Thank you,
", input$userEmail, sep = "")

    sendmailR::sendmail(from, to, subject, body, cc, control = list(smtpServer = Sys.getenv("servidor_SMTP")))
    logger::log_success("(server.R) Withdraw email sent")
    toast("Email sent!", type = "success")
  })

  # Disable by default
  shinyjs::disable("launchMergedResults")

  observeEvent(input$submitCreateMergedResults, {
    # Disable button
    shinyjs::disable("submitCreateMergedResults")


    numberOfSeconds <- 2700 * multiple_databaseCodes_count # 45 mins ## TODO MULTIPLY per Database per Cohort

    # Start Progress Bar
    startProgressTimer(numberOfSeconds * 1000, easing = "linear")

    # Close the Progress Bar
    closeProgressTimer()
    toast("Execution Completed!", type = "success")


    # Enable button
    shinyjs::enable("launchMergedResults")
  })

  sqliteDbPath <- reactiveVal()

  # A button to launch the Cohort Diagnostics App
  observeEvent(
    selected_row(),
    {
      dataBase <- PL_app_dbs$Name[1] # default is 1st one, because it pulls last one from sqlite on S3
      CohortID <- "" # must be null for init

      datatable_rows_selected <- req(input$datatable_rows_selected) # Original: input$datatable_rows_selected

      atlas_user_name <- input$atlas_user_name # Original: input$atlas_user_name
      atlas_password <- input$atlas_password # Original: input$atlas_password
      PL_app_dbs # Original: PL_app_dbs (likely an object or parameter already in the environment)
      S3_bucket_server <- Sys.getenv("S3_bucket_server") # Original: input$S3_bucket_server
      S3_bucket_cdsqlite_path <- Sys.getenv("S3_bucket_cdsqlite_path") # Original: input$S3_bucket_cdsqlite_path
      S3_bucket_region <- Sys.getenv("S3_bucket_region") # Original: input$S3_bucket_region
      run_Environment <- Sys.getenv("run_Environment") # Original: input$run_Environment
      redshift_username <- Sys.getenv("redshift_username") # Original: input$redshift_username
      redshift_password <- Sys.getenv("redshift_password") # Original: input$redshift_password
      session_user <- session$user # Original: session$user

      results <- launch_cohort_evaluation(
        datatable_rows_selected = datatable_rows_selected,
        dataBase = dataBase,
        CohortID = CohortID,
        atlas_user_name = atlas_user_name,
        atlas_password = atlas_password,
        home_page_df = home_page_df(),
        phenotype_details = phenotype_details,
        PL_app_dbs = PL_app_dbs,
        S3_bucket_server = S3_bucket_server,
        S3_bucket_cdsqlite_path = S3_bucket_cdsqlite_path,
        S3_bucket_region = S3_bucket_region,
        run_Environment = run_Environment,
        redshift_username = redshift_username,
        redshift_password = redshift_password,
        session_user = session_user,
        session = session,
        unlink_outputs = FALSE,
        export_dir = export_dir,
        postgresql_schema_name = "phenotype_library"
      )


      codes <- get_database_codes(
        selected_databases = unlist(strsplit(dataBase, ",")),
        PL_app_dbs = PL_app_dbs
      )

      sqlite_db_path <- file.path(
        export_dir,
        codes$multiple_databaseCodes,
        paste0(selected_row()$jnj_cohort_definition_id, ".sqlite")
      )
      checkmate::assert_file_exists(sqlite_db_path)
      sqliteDbPath(sqlite_db_path)

      if (as.character.default(Sys.getenv("run_Environment") == "development")) {
        loggit("INFO", "Set Server Module cdModule: ", app = "server.R")
        loggit("INFO", paste0("connectionHandler Lenght: ", as.character.default(length(connectionHandler))), app = "server.R")
        loggit("INFO", paste0("dataSource Lenght: ", length(as.character.default(length(dataSource)))), app = "server.R")
        loggit("INFO", paste0("shinySettings Lenght: ", as.character.default(length(shinySettings))), app = "server.R")
      }


      # Server Module: Cohort Diagnostics App
      loggit("INFO", "server module -----")
      cdModule <- OhdsiShinyModules::cohortDiagnosticsServer(
        id = "DiagnosticsExplorer",
        connectionHandler = connectionHandler,
        dataSource = dataSource,
        resultDatabaseSettings = shinySettings
      )
      loggit("INFO", "-----")
    } # end of launch
  )

  # Render data table for home page
  output$datatable <- DT::renderDT(
    home_page_df(),
    server = FALSE,
    selection = list(mode = "single")
  )

  # If the Phenotype is selected, launch the Cohort Diagnostics
  observeEvent(input$datatable_rows_selected, {
    print("Auto Click Launch")
    click("Launch")
  })


  observeEvent(input$downloadPhenotypeMetadata, {
    # Get selected phenotype information
    selected_phenotype <- home_page_df()[input$datatable_rows_selected, ]$`Phenotype Name`
    selected_row <- subset(phenotype_details, phenotype_name == selected_phenotype())

    # runjs("$('#downloadMetadata')[0].click();") # DOWNLOAD BUTTON

    # Reactive element to store the clinical code list
    codeListData <- reactive({
      cohort_id <- selected_row()$jnj_cohort_definition_id
      query_code_list <-
        paste(
          "SELECT * FROM",
          clinical_code_lists,
          "WHERE cohort_id = ",
          cohort_id
        ) # Extract the appropriate clinical code list of the selected phenotype
      code_list <- run_query(query_code_list)
    })

    # Display Clinical Code List using renderDT
    output$clinicalCodeList <- renderDT({
      datatable(codeListData(), options = list(pageLength = 10))
    })

    # Downloading Clinical Code List
    output$downloadTable <- downloadHandler(
      filename = function() {
        paste(selected_phenotype(),
          "_ClinicalCodeList_",
          Sys.Date(),
          ".csv",
          sep = ""
        )
      },
      content = function(file) {
        write.csv(codeListData(), file)
      }
    )

    # Metadata Download if the file does not exist on S3
    json_key <- paste(
      "CD_JSON/",
      paste0(selected_row()$jnj_cohort_definition_id, ".json"),
      sep = ""
    )

    json_exists <- aws.s3::object_exists(json_key, Sys.getenv("S3_bucket_server"))
    if (!json_exists) {
      showModal(modalDialog(
        title = "Object Not Found",
        shiny::HTML(glue::glue("Cannot download because object <strong>{json_key}</strong> does not exist in AWS S3 <strong>{Sys.getenv(\"S3_bucket_server\")}</strong>."))
      ))
      req(json_exists)
    }

    CD_JSON <-
      save_object(
        json_key,
        file = paste(
          getwd(),
          paste(selected_row()$jnj_cohort_definition_id, ".json", sep = ""),
          sep = "/"
        ),
        bucket = Sys.getenv("S3_bucket_server")
      )
    to_download <-
      reactiveValues(
        clinical_code_list = codeListData(),
        CD_LD =
          paste(
            paste(
              selected_row()$phenotype_name,
              "\n\n",
              "Clinical Description: \n",
              selected_row()$clinical_description
            ),
            paste(
              selected_row()$phenotype_name,
              "\n",
              "Logic Description: \n\n",
              "Cohort Entry Event:\n",
              selected_row()$cohort_entry_event,
              "\n\n",
              "Index Event:\n",
              selected_row()$index_event,
              "\n\n",
              "Inclusion/Exclusion Criteria:\n",
              selected_row()$ie_criteria,
              "\n\n",
              "Cohort Exit:\n",
              selected_row()$cohort_exit
            ),
            sep = "\n\n"
          ),
        Citation = paste(
          selected_row()$phenotype_name,
          selected_row()$lead_author,
          selected_row()$co_authors,
          sep = ","
        ),
        CD_json = read_json(CD_JSON, simplifyVector = FALSE)
      )

    output$downloadMetadata <- downloadHandler(
      filename = function() {
        paste(
          "PL_",
          selected_row()$phenotype_id,
          "_",
          selected_row()$version_date,
          ".zip",
          sep = ""
        )
      },
      content = function(file) {
        temp_directory <- file.path(getwd(), "metadata")
        dir.create(temp_directory, showWarnings = FALSE) # Avoid warnings if the directory already exists
        reactiveValuesToList(to_download) %>%
          imap(function(x, y) {
            file_extension <- switch(y,
              clinical_code_list = "csv",
              CD_LD = "txt",
              Citation = "txt",
              CD_json = "json",
              "pdf"
            ) # Default to csv if the dataset is not recognized
            file_name <- glue("{y}_data.{file_extension}")
            if (file_extension == "csv") {
              write.csv(x, file.path(temp_directory, file_name))
            } else if (file_extension == "txt") {
              writeLines(x, file.path(temp_directory, file_name))
            } else if (file_extension == "json") {
              write_json(x, file.path(temp_directory, file_name), pretty = TRUE)
            }
          })
        # Use system2 to call the zip command
        system2("zip", args = c(file, paste(
          list.files(temp_directory, full.names = TRUE),
          collapse = " "
        )))
        # Deleting the folder "metadata"
        unlink(temp_directory, recursive = TRUE)
        # Deleting the json file
        unlink(CD_JSON, recursive = TRUE)

        # Refresh after download button so user can choose new Pheontype
        refresh()
      },
      contentType = "application/zip"
    )


    # Render dynamic boxes containing detailed information about the phenotype
    output$dynamic_boxes <- renderUI({
      fluidRow(
        # Box 1: Phenotype Name
        box(
          title = HTML(
            paste(
              "<strong><center>",
              selected_row()$phenotype_name,
              "</center></strong>"
            )
          ),
          width = 12,
          solidheader = TRUE,
          height = 45,
          div(
            style = "position: absolute; top: 5px; right: 10px;",
            # Adjust position as needed
            # # Download Button for Phenotype Metadata
            downloadButton("downloadMetadata", label = "Phenotype Download", style = "color: white; background-color: #222d32; margin-left: 18px;"),
          )
        ),

        # Box 2: Clinical Description

        box(
          title = HTML("<b>Clinical Description</b>"),
          width = 12,
          solidHeader = TRUE,
          height = "auto",
          style = "max-height: 500px; overflow-y: auto;",
          tags$ul(HTML(
            paste0(
              "<p style='font-size: 14px;'>",
              gsub("[\n]", "<br>", selected_row()$clinical_description),
              "</p>"
            )
          )),
          div(
            style = "position: absolute; top: 5px; right: 10px;",
            # Adjust position as needed
            rclipboardSetup(),
            rclipButton(
              inputId = "clipbtn",
              label = "Copy",
              clipText = paste(
                selected_row()$phenotype_name,
                "\n\n",
                "Clinical Description: \n",
                selected_row()$clinical_description
              ),
              icon = icon("clipboard")
            )
          )
        ),

        # Box 3: Logic Description

        box(
          title = HTML("<b>Logic Description:</b>"),
          width = 12,
          solidHeader = TRUE,
          height = "auto",
          style = "max-height: 500px; overflow-y: auto;",
          tags$ul(
            tags$h4(HTML("<b>Cohort Entry Event</b>")),
            tags$ul(HTML(
              paste0(
                "<p style='font-size: 14px;'>",
                gsub("[\n]", "<br>", selected_row()$cohort_entry_event),
                "</p>"
              )
            )),
            tags$h4(HTML("<b>Index Event</b>")),
            tags$ul(HTML(
              paste0(
                "<p style='font-size: 14px;'>",
                gsub("[\n]", "<br>", selected_row()$index_event),
                "</p>"
              )
            )),
            tags$h4(HTML(
              "<b>Inclusion/Exclusion Criteria</b>"
            )),
            tags$ul(HTML(
              paste0(
                "<p style='font-size: 14px;'>",
                gsub("[\n]", "<br>", selected_row()$ie_criteria),
                "</p>"
              )
            )),
            tags$h4(HTML("<b>Cohort Exit</b>")),
            tags$ul(HTML(
              paste0(
                "<p style='font-size: 14px;'>",
                gsub("[\n]", "<br>", selected_row()$cohort_exit),
                "</p>"
              )
            ))
          ),
          div(
            style = "position: absolute; top: 5px; right: 10px;",
            # Adjust position as needed
            rclipboardSetup(),
            rclipButton(
              inputId = "clipbtn",
              label = "Copy",
              clipText = paste(
                selected_row()$phenotype_name,
                "\n",
                "Logic Description: \n\n",
                "Cohort Entry Event:\n",
                selected_row()$cohort_entry_event,
                "\n\n",
                "Index Event:\n",
                selected_row()$index_event,
                "\n\n",
                "Inclusion/Exclusion Criteria:\n",
                selected_row()$ie_criteria,
                "\n\n",
                "Cohort Exit:\n",
                selected_row()$cohort_exit
              ),
              icon = icon("clipboard")
            )
          )
        ),

        # Box 4: Clinical code lists

        box(
          title = HTML("<b>Clinical Code Lists</b>"),
          width = 12,
          solidHeader = TRUE,
          height = "auto",
          style = "max-height: 800px; overflow-y: auto;",
          DTOutput("clinicalCodeList1"),
          downloadButton("downloadTable", label = "Download Table"),
        ),

        # Box 5: Authors

        box(
          title = HTML("<b>Authors</b>"),
          width = 12,
          solidHeader = TRUE,
          height = "auto",
          style = "max-height: 300px; overflow-y: auto;",
          tags$ul(
            HTML(
              paste0(
                "<p style='font-size: 14px;'><strong>",
                gsub("[\n]", "<br>", selected_row()$lead_author),
                "</strong></p>"
              )
            ),
            HTML(
              paste0(
                "<p style='font-size: 14px;'>",
                gsub("[\n]", "<br>", selected_row()$co_authors),
                "</p>"
              )
            ),
          )
        ),

        # Box 6: References

        box(
          title = HTML("<b>References</b>"),
          width = 12,
          solidHeader = TRUE,
          height = "auto",
          HTML(paste0(
            "<p style='font-size: 14px;'>",
            gsub("[\n]", "<br>", selected_row()$reference_links),
            "</p>"
          )),
          style = "overflow-x: hidden; overflow-y: hidden;"
        ),
        box(
          title = HTML("<b>Citation</b>"),
          width = 12,
          solidHeader = TRUE,
          height = "auto",
          HTML(
            paste(
              selected_row()$phenotype_name,
              selected_row()$lead_author,
              selected_row()$co_authors,
              sep = ","
            )
          ),
          style = "overflow-x: hidden; overflow-y: hidden;",
          div(
            style = "position: absolute; top: 5px; right: 10px;",
            # Adjust position as needed
            rclipboardSetup(),
            rclipButton(
              inputId = "clipbtn",
              label = "Copy",
              clipText = paste(
                selected_row()$phenotype_name,
                selected_row()$lead_author,
                selected_row()$co_authors,
                sep = ","
              ),
              icon = icon("clipboard")
            )
          )
        )
      )
    })

    output$dynamic_boxes_AuthorsRefrencesCitations <- renderUI({
      fluidRow(
        # Box 1: Phenotype Name
        box(
          title = HTML(
            paste(
              "<strong><center>",
              selected_row()$phenotype_name,
              "</center></strong>"
            )
          ),
          width = 12,
          solidheader = TRUE,
          height = 37
        ),
        # Box 2: Authors
        box(
          title = HTML("<b>Authors</b>"),
          width = 12,
          solidHeader = TRUE,
          height = "auto",
          style = "max-height: 300px; overflow-y: auto;",
          tags$ul(
            HTML(
              paste0(
                "<p style='font-size: 14px;'><strong>",
                gsub("[\n]", "<br>", selected_row()$lead_author),
                "</strong></p>"
              )
            ),
            HTML(
              paste0(
                "<p style='font-size: 14px;'>",
                gsub("[\n]", "<br>", selected_row()$co_authors),
                "</p>"
              )
            ),
          )
        ),
        # Box 6: References
        box(
          title = HTML("<b>References</b>"),
          width = 12,
          solidHeader = TRUE,
          height = "auto",
          HTML(paste0(
            "<p style='font-size: 14px;'>",
            gsub("[\n]", "<br>", selected_row()$reference_links),
            "</p>"
          )),
          style = "overflow-x: hidden; overflow-y: hidden;"
        ),
        box(
          title = HTML("<b>Citation</b>"),
          width = 12,
          solidHeader = TRUE,
          height = "auto",
          HTML(
            paste(
              selected_row()$phenotype_name,
              selected_row()$lead_author,
              selected_row()$co_authors,
              sep = ","
            )
          ),
          style = "overflow-x: hidden; overflow-y: hidden;",
          div(
            style = "position: absolute; top: 5px; right: 10px;",
            # Adjust position as needed
            rclipboardSetup(),
            rclipButton(
              inputId = "clipbtn",
              label = "Copy",
              clipText = paste(
                selected_row()$phenotype_name,
                selected_row()$lead_author,
                selected_row()$co_authors,
                sep = ","
              ),
              icon = icon("clipboard")
            )
          )
        ),
      )
    }) # end of render UI

    # Render Descriptions
    output$dynamic_boxes_clinicalDescription <- renderUI({
      fluidRow(
        # Box 1: Phenotype Name
        box(
          title = HTML(
            paste(
              "<strong><center>",
              selected_row()$phenotype_name,
              "</center></strong>"
            )
          ),
          width = 12,
          solidheader = TRUE,
          height = 37
        ),

        # Box 3: Clinical Description
        box(
          title = HTML("<b>Clinical Description</b>"),
          width = 12,
          solidHeader = TRUE,
          height = "auto",
          style = "max-height: 500px; overflow-y: auto;",
          tags$ul(HTML(
            paste0(
              "<p style='font-size: 14px;'>",
              gsub("[\n]", "<br>", selected_row()$clinical_description),
              "</p>"
            )
          ), ),
          div(
            style = "position: absolute; top: 5px; right: 10px;",
            # Adjust position as needed
            rclipboardSetup(),
            rclipButton(
              inputId = "clipbtn",
              label = "Copy",
              clipText = paste(
                selected_row()$phenotype_name,
                "\n\n",
                "Clinical Description: \n",
                selected_row()$clinical_description
              ),
              icon = icon("clipboard")
            )
          )
        )
      )
    }) # end of render UI

    # Render Logic Description

    # Render dynamic boxes containing detailed information about the phenotype
    output$dynamic_boxes_LogicDescription <- renderUI({
      fluidRow(
        # Box 1: Phenotype Name
        box(
          title = HTML(
            paste(
              "<strong><center>",
              selected_row()$phenotype_name,
              "</center></strong>"
            )
          ),
          width = 12,
          solidheader = TRUE,
          height = 37
        ),
        # Box 4: Logic Description
        box(
          title = HTML("<b>Logic Description:</b>"),
          width = 12,
          solidHeader = TRUE,
          height = "auto",
          style = "max-height: 500px; overflow-y: auto;",
          tags$ul(
            tags$h4(HTML("<b>Cohort Entry Event</b>")),
            tags$ul(HTML(
              paste0(
                "<p style='font-size: 14px;'>",
                gsub("[\n]", "<br>", selected_row()$cohort_entry_event),
                "</p>"
              )
            )),
            tags$h4(HTML("<b>Index Event</b>")),
            tags$ul(HTML(
              paste0(
                "<p style='font-size: 14px;'>",
                gsub("[\n]", "<br>", selected_row()$index_event),
                "</p>"
              )
            )),
            tags$h4(HTML(
              "<b>Inclusion/Exclusion Criteria</b>"
            )),
            tags$ul(HTML(
              paste0(
                "<p style='font-size: 14px;'>",
                gsub("[\n]", "<br>", selected_row()$ie_criteria),
                "</p>"
              )
            )),
            tags$h4(HTML("<b>Cohort Exit</b>")),
            tags$ul(HTML(
              paste0(
                "<p style='font-size: 14px;'>",
                gsub("[\n]", "<br>", selected_row()$cohort_exit),
                "</p>"
              )
            ))
          ),
          div(
            style = "position: absolute; top: 5px; right: 10px;",
            # Adjust position as needed
            rclipboardSetup(),
            rclipButton(
              inputId = "clipbtn",
              label = "Copy",
              clipText = paste(
                selected_row()$phenotype_name,
                "\n",
                "Logic Description: \n\n",
                "Cohort Entry Event:\n",
                selected_row()$cohort_entry_event,
                "\n\n",
                "Index Event:\n",
                selected_row()$index_event,
                "\n\n",
                "Inclusion/Exclusion Criteria:\n",
                selected_row()$ie_criteria,
                "\n\n",
                "Cohort Exit:\n",
                selected_row()$cohort_exit
              ),
              icon = icon("clipboard")
            )
          )
        ),
      )
    }) # end of render UI


    # Render Clinical Code List

    # Render dynamic boxes containing detailed information about the phenotype
    output$dynamic_boxes_ClinicalCodeList <- renderUI({
      fluidRow(
        # Box 1: Phenotype Name
        box(
          title = HTML(
            paste(
              "<strong><center>",
              selected_row()$phenotype_name,
              "</center></strong>"
            )
          ),
          width = 12,
          solidheader = TRUE,
          height = 37
        ),
        # Box 5: Clinical Code Lists
        box(
          title = HTML("<b>Clinical Code Lists</b>"),
          width = 12,
          solidHeader = TRUE,
          height = "auto",
          style = "max-height: 800px; overflow-y: auto;",
          DTOutput("clinicalCodeList"),
          downloadButton("downloadTable", label = "Download Table"),
        ),
        # # Download Button for Phenotype Metadata
        # downloadButton("downloadMetadata", label = "Phenotype Download", style = "color: white; background-color: #222d32; margin-left: 18px;")
      )
    }) # end of render UI

    updateTabItems(session, "tabs", selected = "home")
  })

  # Get selected phenotype information
  selected_phenotype <- reactive({
    home_page_df()[input$datatable_rows_selected, ]$`Phenotype Name`
  })
  selected_row <- reactive({
    subset(phenotype_details, phenotype_name == selected_phenotype())
  })

  # Reactive element to store the clinical code list
  codeListData <- reactive({
    cohort_id <- selected_row()$jnj_cohort_definition_id
    query_code_list <-
      paste(
        "SELECT * FROM",
        clinical_code_lists,
        "WHERE cohort_id = ",
        cohort_id
      ) # Extract the appropriate clinical code list of the selected phenotype
    code_list <- run_query(query_code_list)
  })

  # Display Clinical Code List using renderDT
  output$clinicalCodeList <- renderDT({
    datatable(codeListData(), options = list(pageLength = 10))
  })

  output$clinicalCodeList1 <- renderDT({
    datatable(codeListData(), options = list(pageLength = 10))
  })

  # Downloading Clinical Code List
  output$downloadTable <- downloadHandler(
    filename = function() {
      paste(selected_phenotype(),
        "_ClinicalCodeList_",
        Sys.Date(),
        ".csv",
        sep = ""
      )
    },
    content = function(file) {
      write.csv(codeListData(), file)
    }
  )

  # Metadata Download if the file does not exist on S3
  CD_JSON <-
    reactive({
      save_object(
        paste(
          "CD_JSON/",
          paste0(selected_row()$jnj_cohort_definition_id, ".json"),
          sep = ""
        ),
        file = paste(
          getwd(),
          paste(selected_row()$jnj_cohort_definition_id, ".json", sep = ""),
          sep = "/"
        ),
        bucket = Sys.getenv("S3_bucket_server")
      )
    })
  to_download <-
    reactive({
      clinical_code_list <- codeListData()
      CD_LD <-
        paste(
          paste(
            selected_row()$phenotype_name,
            "\n\n",
            "Clinical Description: \n",
            selected_row()$clinical_description
          ),
          paste(
            selected_row()$phenotype_name,
            "\n",
            "Logic Description: \n\n",
            "Cohort Entry Event:\n",
            selected_row()$cohort_entry_event,
            "\n\n",
            "Index Event:\n",
            selected_row()$index_event,
            "\n\n",
            "Inclusion/Exclusion Criteria:\n",
            selected_row()$ie_criteria,
            "\n\n",
            "Cohort Exit:\n",
            selected_row()$cohort_exit
          ),
          sep = "\n\n"
        )
      Citation <- paste(
        selected_row()$phenotype_name,
        selected_row()$lead_author,
        selected_row()$co_authors,
        sep = ","
      )
      CD_json <- reactive({
        read_json(CD_JSON(), simplifyVector = FALSE)
      })
    })

  output$downloadMetadata <- downloadHandler(
    filename = function() {
      paste(
        "PL_",
        selected_row()$phenotype_id,
        "_",
        selected_row()$version_date,
        ".zip",
        sep = ""
      )
    },
    content = function(file) {
      temp_directory <- file.path(getwd(), "metadata")
      dir.create(temp_directory, showWarnings = FALSE) # Avoid warnings if the directory already exists
      reactiveValuesToList(to_download) %>%
        imap(function(x, y) {
          file_extension <- switch(y,
            clinical_code_list = "csv",
            CD_LD = "txt",
            Citation = "txt",
            CD_json = "json",
            "pdf"
          ) # Default to csv if the dataset is not recognized
          file_name <- glue("{y}_data.{file_extension}")
          if (file_extension == "csv") {
            write.csv(x, file.path(temp_directory, file_name))
          } else if (file_extension == "txt") {
            writeLines(x, file.path(temp_directory, file_name))
          } else if (file_extension == "json") {
            write_json(x, file.path(temp_directory, file_name), pretty = TRUE)
          }
        })
      # Use system2 to call the zip command
      system2("zip", args = c(file, paste(
        list.files(temp_directory, full.names = TRUE),
        collapse = " "
      )))
      # Deleting the folder "metadata"
      unlink(temp_directory, recursive = TRUE)
      # Deleting the json file
      unlink(CD_JSON(), recursive = TRUE)
    },
    contentType = "application/zip"
  )

  # Render dynamic boxes containing detailed information about the phenotype

  output$dynamic_boxes <- renderUI({
    fluidRow(
      # Box 1: Phenotype Name
      box(
        title = HTML(
          paste(
            "<strong><center>",
            selected_row()$phenotype_name,
            "</center></strong>"
          )
        ),
        width = 12,
        solidheader = TRUE,
        height = 45,
        div(
          style = "position: absolute; top: 5px; right: 10px;",
          conditionalPanel(
            "false", # always hide the download button
            downloadButton("downloadMetadata", label = "Phenotype Download", style = "color: white; background-color: #222d32; margin-left: 18px;")
          ),
          actionButton(inputId = "downloadPhenotypeMetadata", label = "Phenotype Download", style = "color: white; background-color: #222d32; margin-left: 18px;", icon = icon("download")),
        )
      ),

      # Box 2: Clinical Description

      box(
        title = HTML("<b>Clinical Description</b>"),
        width = 12,
        solidHeader = TRUE,
        height = "auto",
        style = "max-height: 500px; overflow-y: auto;",
        tags$ul(HTML(
          paste0(
            "<p style='font-size: 14px;'>",
            gsub("[\n]", "<br>", selected_row()$clinical_description),
            "</p>"
          )
        )),
        div(
          style = "position: absolute; top: 5px; right: 10px;",
          # Adjust position as needed
          rclipboardSetup(),
          rclipButton(
            inputId = "clipbtn",
            label = "Copy",
            clipText = paste(
              selected_row()$phenotype_name,
              "\n\n",
              "Clinical Description: \n",
              selected_row()$clinical_description
            ),
            icon = icon("clipboard")
          )
        )
      ),

      # Box 3: Logic Description

      box(
        title = HTML("<b>Logic Description:</b>"),
        width = 12,
        solidHeader = TRUE,
        height = "auto",
        style = "max-height: 500px; overflow-y: auto;",
        tags$ul(
          tags$h4(HTML("<b>Cohort Entry Event</b>")),
          tags$ul(HTML(
            paste0(
              "<p style='font-size: 14px;'>",
              gsub("[\n]", "<br>", selected_row()$cohort_entry_event),
              "</p>"
            )
          )),
          tags$h4(HTML("<b>Index Event</b>")),
          tags$ul(HTML(
            paste0(
              "<p style='font-size: 14px;'>",
              gsub("[\n]", "<br>", selected_row()$index_event),
              "</p>"
            )
          )),
          tags$h4(HTML(
            "<b>Inclusion/Exclusion Criteria</b>"
          )),
          tags$ul(HTML(
            paste0(
              "<p style='font-size: 14px;'>",
              gsub("[\n]", "<br>", selected_row()$ie_criteria),
              "</p>"
            )
          )),
          tags$h4(HTML("<b>Cohort Exit</b>")),
          tags$ul(HTML(
            paste0(
              "<p style='font-size: 14px;'>",
              gsub("[\n]", "<br>", selected_row()$cohort_exit),
              "</p>"
            )
          ))
        ),
        div(
          style = "position: absolute; top: 5px; right: 10px;",
          # Adjust position as needed
          rclipboardSetup(),
          rclipButton(
            inputId = "clipbtn",
            label = "Copy",
            clipText = paste(
              selected_row()$phenotype_name,
              "\n",
              "Logic Description: \n\n",
              "Cohort Entry Event:\n",
              selected_row()$cohort_entry_event,
              "\n\n",
              "Index Event:\n",
              selected_row()$index_event,
              "\n\n",
              "Inclusion/Exclusion Criteria:\n",
              selected_row()$ie_criteria,
              "\n\n",
              "Cohort Exit:\n",
              selected_row()$cohort_exit
            ),
            icon = icon("clipboard")
          )
        )
      ),

      # Box 4: Clinical code lists

      box(
        title = HTML("<b>Clinical Code Lists</b>"),
        width = 12,
        solidHeader = TRUE,
        height = "auto",
        style = "max-height: 800px; overflow-y: auto;",
        DTOutput("clinicalCodeList1"),
        downloadButton("downloadTable", label = "Download Table"),
      ),

      # Box 5: Authors

      box(
        title = HTML("<b>Authors</b>"),
        width = 12,
        solidHeader = TRUE,
        height = "auto",
        style = "max-height: 300px; overflow-y: auto;",
        tags$ul(
          HTML(
            paste0(
              "<p style='font-size: 14px;'><strong>",
              gsub("[\n]", "<br>", selected_row()$lead_author),
              "</strong></p>"
            )
          ),
          HTML(
            paste0(
              "<p style='font-size: 14px;'>",
              gsub("[\n]", "<br>", selected_row()$co_authors),
              "</p>"
            )
          ),
        )
      ),

      # Box 6: References

      box(
        title = HTML("<b>References</b>"),
        width = 12,
        solidHeader = TRUE,
        height = "auto",
        HTML(paste0(
          "<p style='font-size: 14px;'>",
          gsub("[\n]", "<br>", selected_row()$reference_links),
          "</p>"
        )),
        style = "overflow-x: hidden; overflow-y: hidden;"
      ),
      box(
        title = HTML("<b>Citation</b>"),
        width = 12,
        solidHeader = TRUE,
        height = "auto",
        HTML(
          paste(
            selected_row()$phenotype_name,
            selected_row()$lead_author,
            selected_row()$co_authors,
            sep = ","
          )
        ),
        style = "overflow-x: hidden; overflow-y: hidden;",
        div(
          style = "position: absolute; top: 5px; right: 10px;",
          # Adjust position as needed
          rclipboardSetup(),
          rclipButton(
            inputId = "clipbtn",
            label = "Copy",
            clipText = paste(
              selected_row()$phenotype_name,
              selected_row()$lead_author,
              selected_row()$co_authors,
              sep = ","
            ),
            icon = icon("clipboard")
          )
        )
      )
    )
  })

  output$dynamic_boxes_AuthorsRefrencesCitations <- renderUI({
    fluidRow(
      # Box 1: Phenotype Name
      box(
        title = HTML(
          paste(
            "<strong><center>",
            selected_row()$phenotype_name,
            "</center></strong>"
          )
        ),
        width = 12,
        solidheader = TRUE,
        height = 37
      ),
      # Box 2: Authors
      box(
        title = HTML("<b>Authors</b>"),
        width = 12,
        solidHeader = TRUE,
        height = "auto",
        style = "max-height: 300px; overflow-y: auto;",
        tags$ul(
          HTML(
            paste0(
              "<p style='font-size: 14px;'><strong>",
              gsub("[\n]", "<br>", selected_row()$lead_author),
              "</strong></p>"
            )
          ),
          HTML(
            paste0(
              "<p style='font-size: 14px;'>",
              gsub("[\n]", "<br>", selected_row()$co_authors),
              "</p>"
            )
          ),
        )
      ),
      # Box 6: References
      box(
        title = HTML("<b>References</b>"),
        width = 12,
        solidHeader = TRUE,
        height = "auto",
        HTML(paste0(
          "<p style='font-size: 14px;'>",
          gsub("[\n]", "<br>", selected_row()$reference_links),
          "</p>"
        )),
        style = "overflow-x: hidden; overflow-y: hidden;"
      ),
      box(
        title = HTML("<b>Citation</b>"),
        width = 12,
        solidHeader = TRUE,
        height = "auto",
        HTML(
          paste(
            selected_row()$phenotype_name,
            selected_row()$lead_author,
            selected_row()$co_authors,
            sep = ","
          )
        ),
        style = "overflow-x: hidden; overflow-y: hidden;",
        div(
          style = "position: absolute; top: 5px; right: 10px;",
          # Adjust position as needed
          rclipboardSetup(),
          rclipButton(
            inputId = "clipbtn",
            label = "Copy",
            clipText = paste(
              selected_row()$phenotype_name,
              selected_row()$lead_author,
              selected_row()$co_authors,
              sep = ","
            ),
            icon = icon("clipboard")
          )
        )
      ),
    )
  }) # end of render UI

  # Render Descriptions
  output$dynamic_boxes_clinicalDescription <- renderUI({
    fluidRow(
      # Box 1: Phenotype Name
      box(
        title = HTML(
          paste(
            "<strong><center>",
            selected_row()$phenotype_name,
            "</center></strong>"
          )
        ),
        width = 12,
        solidheader = TRUE,
        height = 37
      ),

      # Box 3: Clinical Description
      box(
        title = HTML("<b>Clinical Description</b>"),
        width = 12,
        solidHeader = TRUE,
        height = "auto",
        style = "max-height: 500px; overflow-y: auto;",
        tags$ul(HTML(
          paste0(
            "<p style='font-size: 14px;'>",
            gsub("[\n]", "<br>", selected_row()$clinical_description),
            "</p>"
          )
        ), ),
        div(
          style = "position: absolute; top: 5px; right: 10px;",
          # Adjust position as needed
          rclipboardSetup(),
          rclipButton(
            inputId = "clipbtn",
            label = "Copy",
            clipText = paste(
              selected_row()$phenotype_name,
              "\n\n",
              "Clinical Description: \n",
              selected_row()$clinical_description
            ),
            icon = icon("clipboard")
          )
        )
      )
    )
  }) # end of render UI

  # Render Logic Description

  # Render dynamic boxes containing detailed information about the phenotype
  output$dynamic_boxes_LogicDescription <- renderUI({
    fluidRow(
      # Box 1: Phenotype Name
      box(
        title = HTML(
          paste(
            "<strong><center>",
            selected_row()$phenotype_name,
            "</center></strong>"
          )
        ),
        width = 12,
        solidheader = TRUE,
        height = 37
      ),
      # Box 4: Logic Description
      box(
        title = HTML("<b>Logic Description:</b>"),
        width = 12,
        solidHeader = TRUE,
        height = "auto",
        style = "max-height: 500px; overflow-y: auto;",
        tags$ul(
          tags$h4(HTML("<b>Cohort Entry Event</b>")),
          tags$ul(HTML(
            paste0(
              "<p style='font-size: 14px;'>",
              gsub("[\n]", "<br>", selected_row()$cohort_entry_event),
              "</p>"
            )
          )),
          tags$h4(HTML("<b>Index Event</b>")),
          tags$ul(HTML(
            paste0(
              "<p style='font-size: 14px;'>",
              gsub("[\n]", "<br>", selected_row()$index_event),
              "</p>"
            )
          )),
          tags$h4(HTML(
            "<b>Inclusion/Exclusion Criteria</b>"
          )),
          tags$ul(HTML(
            paste0(
              "<p style='font-size: 14px;'>",
              gsub("[\n]", "<br>", selected_row()$ie_criteria),
              "</p>"
            )
          )),
          tags$h4(HTML("<b>Cohort Exit</b>")),
          tags$ul(HTML(
            paste0(
              "<p style='font-size: 14px;'>",
              gsub("[\n]", "<br>", selected_row()$cohort_exit),
              "</p>"
            )
          ))
        ),
        div(
          style = "position: absolute; top: 5px; right: 10px;",
          # Adjust position as needed
          rclipboardSetup(),
          rclipButton(
            inputId = "clipbtn",
            label = "Copy",
            clipText = paste(
              selected_row()$phenotype_name,
              "\n",
              "Logic Description: \n\n",
              "Cohort Entry Event:\n",
              selected_row()$cohort_entry_event,
              "\n\n",
              "Index Event:\n",
              selected_row()$index_event,
              "\n\n",
              "Inclusion/Exclusion Criteria:\n",
              selected_row()$ie_criteria,
              "\n\n",
              "Cohort Exit:\n",
              selected_row()$cohort_exit
            ),
            icon = icon("clipboard")
          )
        )
      )
    )
  }) # end of render UI


  # Render Clinical Code List

  # Render dynamic boxes containing detailed information about the phenotype
  output$dynamic_boxes_ClinicalCodeList <- renderUI({
    fluidRow(
      # Box 1: Phenotype Name
      box(
        title = HTML(
          paste(
            "<strong><center>",
            selected_row()$phenotype_name,
            "</center></strong>"
          )
        ),
        width = 12,
        solidheader = TRUE,
        height = 37
      ),
      # Box 5: Clinical Code Lists
      box(
        title = HTML("<b>Clinical Code Lists</b>"),
        width = 12,
        solidHeader = TRUE,
        height = "auto",
        style = "max-height: 800px; overflow-y: auto;",
        DTOutput("clinicalCodeList"),
        downloadButton("downloadTable", label = "Download Table"),
      ),
    )
  }) # end of render UI

  # Get the selected Requested Phenotype to display all of the data below.

  sql_get_all_requested_phenotype_details <- "Select phenotyperequest_id, phenotype_name, lead_author, co_authors, tags, reference_links, clinical_description, cohort_entry_event, index_event, ie_criteria, cohort_exit, db_interest, jnj_cohort_definition_id, clinical_code_list_json_csv, request_by_user, CONCAT(CONCAT(request_date, ' - '), request_time) AS request_date_time from PHENOTYPE_LIBRARY.requested_phenotype_details;"
  sql_get_grid_requested_phenotype_details <- "Select rpd.phenotyperequest_id, rpr2.status, phenotype_name, lead_author, clinical_description,  jnj_cohort_definition_id, request_by_user, CONCAT(CONCAT(request_date, ' - '), request_time) AS request_date_time, comments from PHENOTYPE_LIBRARY.requested_phenotype_details rpd LEFT OUTER JOIN (select  rpr2.phenotyperequest_id, max(rpr2.phenotyperequestresponse_id) phenotyperequestresponse_id from PHENOTYPE_LIBRARY.requested_phenotype_response rpr2 group by phenotyperequest_id) lastSD on lastSD.phenotyperequest_id = rpd.phenotyperequest_id left join PHENOTYPE_LIBRARY.requested_phenotype_response rpr2 on rpr2.phenotyperequest_id = lastSD.phenotyperequest_id and rpr2.phenotyperequestresponse_id = lastSD.phenotyperequestresponse_id order by rpd.phenotyperequest_id; "

  requested_phenotype_details <- run_query(sql_get_all_requested_phenotype_details)
  grid_requested_phenotype_details <- run_query(sql_get_grid_requested_phenotype_details)

  requestedphenotype_page_df <- data.frame(requested_phenotype_details)
  colnames(requestedphenotype_page_df) <-
    c(
      "Phenotype Request ID",
      "Phenotype Name",
      "Lead Author",
      "Co Authors",
      "Tags",
      "References",
      "Clinical Description",
      "Cohort Entry Event",
      "Index Event",
      "Inclusion/Exclusion Criteria",
      "Cohort Exit",
      "Databases of Interest",
      "Cohort ID",
      "Clinical Code List",
      "Requested By",
      "Requested Date"
    )

  grid_requestedphenotype_page_df <- data.frame(grid_requested_phenotype_details)
  colnames(grid_requestedphenotype_page_df) <-
    c(
      "Phenotype Request ID",
      "Status",
      "Phenotype Name",
      "Lead Author",
      # "Co Authors",
      # "Tags",
      # "References",
      "Clinical Description",
      # "Cohort Entry Event",
      # "Index Event",
      # "Inclusion/Exclusion Criteria",
      # "Cohort Exit",
      # "Databases of Interest",
      "Cohort ID",
      # "Clinical Code List",
      "Last Modified By",
      "Last Modified Date",
      "Comments"
    )


  rv <- reactiveVal(grid_requestedphenotype_page_df)

  output$phenotyperequestsdatatable <- DT::renderDT(
    DT::datatable(
      rv(),
      selection = list(mode = "single", selected = c(1)),
      extensions = "Buttons",
      options = list(
        autoWidth = FALSE,
        scrollX = TRUE,
        dom = "Bfrtip",
        pageLength = 5
      )
    )
  )


  # Get selected phenotype information
  selected_requested_phenotype <- reactive({
    grid_requestedphenotype_page_df[input$phenotyperequestsdatatable_rows_selected, ]$`Phenotype Request ID`
  })
  selected_row_requested_phenotype <- reactive({
    subset(requested_phenotype_details, phenotyperequest_id == selected_requested_phenotype())
  })
  selected_row_requested_phenotype_status <- reactive({
    subset(grid_requested_phenotype_details, phenotyperequest_id == selected_requested_phenotype())
  })


  # Reactive element to store the clinical code list
  requestedCodeListData <- reactive({
    request_cohort_id <- selected_row_requested_phenotype()$jnj_cohort_definition_id
    if (is.null(request_cohort_id) || request_cohort_id == "") {
      request_cohort_id <- selected_row()$jnj_cohort_definition_id
    }
    query_code_list <-
      paste(
        "SELECT * FROM phenotype_library.requested_phenotype_clinical_code_list ",
        "WHERE cohort_id = ",
        request_cohort_id
      ) # Extract the appropriate clinical code list of the selected phenotype
    request_code_list <- run_query(query_code_list)
  })

  # Display Clinical Code List using renderDT
  output$requestclinicalCodeList <- renderDT({
    datatable(requestedCodeListData(), options = list(pageLength = 10))
  })


  # Disable buttons if selected phenotype is already approved or rejected.
  observeEvent(input$phenotyperequestsdatatable_rows_selected, {
    selected_row_requested_phenotype_status <- {
      grid_requestedphenotype_page_df[input$phenotyperequestsdatatable_rows_selected, ]$`Status`
    }

    print(paste(selected_row_requested_phenotype_status))

    # if selected status is APPROVED, disable the buttons
    if ((paste(selected_row_requested_phenotype_status) == "APPROVED") | (paste(selected_row_requested_phenotype_status) == "REJECTED")) {
      print("Status is approved or rejected for this selected row")
      # Disable the buttons
      shinyjs::disable("approvePhenotype")
      shinyjs::disable("rejectPhenotype")
      shinyjs::disable("requestMoreInfoPhenotype")
    } else {
      shinyjs::enable("approvePhenotype")
      shinyjs::enable("rejectPhenotype")
      shinyjs::enable("requestMoreInfoPhenotype")
    }
  })

  # Render dynamic boxes containing detailed information about the phenotype
  output$requested_phenotype_dynamic_boxes <- renderUI({
    fluidRow(
      column(
        6,
        h4(paste("Phenotype Name: ")), strong(selected_row_requested_phenotype()$phenotype_name),
        h4(paste("Lead Author: ")), strong(selected_row_requested_phenotype()$lead_author),
        h4(paste("Co-Authors: ")), strong(selected_row_requested_phenotype()$co_authors),
        h4(paste("Tags: ")), strong(selected_row_requested_phenotype()$tags),
        h4(paste("References: ")), strong(selected_row_requested_phenotype()$reference_links),
        h4(paste("Clinical Description: ")), strong(selected_row_requested_phenotype()$clinical_description),
        h4(paste("Cohort Entry Event: ")), strong(selected_row_requested_phenotype()$cohort_entry_event),
        h4(paste("Index Event: ")), strong(selected_row_requested_phenotype()$index_event)
      ),
      column(
        6,
        h4(paste("Inclusion/Exclusion Criteria: ")), strong(selected_row_requested_phenotype()$ie_criteria),
        h4(paste("Cohort Exit: ")), strong(selected_row_requested_phenotype()$cohort_exit),
        h4(paste("Databases of Interest: ")), strong(selected_row_requested_phenotype()$db_interest),
        h4(paste("ATLAS Cohort Definition ID: ")), strong(selected_row_requested_phenotype()$jnj_cohort_definition_id),
        h4(paste("Clinical Code List: ")),
        DTOutput("requestclinicalCodeList"),
        h4(paste("Requested By: ")), strong(selected_row_requested_phenotype()$request_by_user),
        h4(paste("Requested Date: ")), strong(paste(selected_row_requested_phenotype()$request_date, " ", selected_row_requested_phenotype()$request_time))
      )
    )
  })

  # Downloading Clinical Code List
  output$download_phenotype_jsoncsv <- downloadHandler(
    filename = function() {
      paste("RequestedPhenotype_", selected_row_requested_phenotype()$phenotype_name,
        "_ClinicalCodeList_",
        Sys.Date(),
        ".txt",
        sep = ""
      )
    },
    content = function(file) {
      writeLines(paste(selected_row_requested_phenotype()$clinical_code_list_json_csv), file)
    }
  )

  observeEvent(input$refreshRequests,
    {
      refresh()
    },
    ignoreInit = TRUE
  )


  observeEvent(input$approvePhenotype, {
    print("add approve code here")

    # Collect Comments from User and save to Database
    showModal(modalDialog(
      tags$h2("Please enter comments for your Approval"),
      textInput("approveComments", "Comments"),
      footer = tagList(
        actionButton("submitApproval", "Submit"),
        modalButton("cancel")
      )
    ))
  })

  # record the approval and comments to db
  observeEvent(input$submitApproval, {
    removeModal()
    # Record the data to requested phenotype response tabe in db
    print("recording approval to database PHENOTYPE_LIBRARY.requested_phenotype_response...")
    print(input$approveComments)

    sql_insertIntoClinicalCodeList <- paste("INSERT INTO PHENOTYPE_LIBRARY.phenotype_clinical_code_list (  cohort_id,  concept_id,  concept_name,  concept_code,  domain_id,  vocabulary_id) SELECT cohort_id, concept_id, concept_name, concept_code, domain_id, vocabulary_id FROM PHENOTYPE_LIBRARY.requested_phenotype_clinical_code_list where cohort_id = ", selected_row_requested_phenotype()$jnj_cohort_definition_id, " ; ", sep = "")


    # Run insert query to add request to the db table and increment the Phenotype_Id
    save_request_phenotype_data <-
      paste(
        paste(
          "INSERT INTO PHENOTYPE_LIBRARY.phenotype_details ( phenotype_id, phenotype_name,  lead_author,  co_authors,  tags,  reference_links,  clinical_description,  cohort_entry_event,  index_event,  ie_criteria,  cohort_exit,  db_interest,  jnj_cohort_definition_id,  version_date) VALUES((SELECT  CASE    WHEN right(max(phenotype_id),4)::bigint < 9 THEN Concat('P000', right(max(phenotype_id),4)::bigint +1)       WHEN right(max(phenotype_id),4)::bigint >= 9 and right(max(phenotype_id),4)::bigint < 99 THEN Concat('P00', right(max(phenotype_id),4)::bigint +1)     ELSE Concat('P0', right(max(phenotype_id),4)::bigint +1)   END  FROM PHENOTYPE_LIBRARY.phenotype_details) ,'",
          paste(selected_row_requested_phenotype()$phenotype_name, selected_row_requested_phenotype()$lead_author,
            selected_row_requested_phenotype()$co_aut, selected_row_requested_phenotype()$tags, selected_row_requested_phenotype()$reference_links,
            selected_row_requested_phenotype()$clinical_description, selected_row_requested_phenotype()$cohort_entry_event, selected_row_requested_phenotype()$index_event,
            selected_row_requested_phenotype()$ie_criteria, selected_row_requested_phenotype()$cohort_exit, selected_row_requested_phenotype()$db_interest, selected_row_requested_phenotype()$jnj_cohort_definition_id,
            sep = "','"
          ), "','", Sys.time(), "');"
        ),
        paste("INSERT INTO PHENOTYPE_LIBRARY.requested_phenotype_response(  phenotyperequest_id,  status,  reviewedby,  review_date,  review_time, comments) VALUES('", selected_row_requested_phenotype()$phenotyperequest_id, "','APPROVED','", session$user, "','", Sys.Date(), "','", Sys.time(), "','", str_replace(input$approveComments, "'", "''"), "' );", sep = ""),
        sep = ""
      )

    print("Save Requested Phenotype to Database")

    execute_command(save_request_phenotype_data)

    # Save Clinical Code List
    save_request_phenotype_data <- paste(sql_insertIntoClinicalCodeList)
    execute_command(save_request_phenotype_data)

    # send email to requestor

    from <- sprintf(Sys.getenv("app_email")) # the sender’s name is an optional value
    to <- sprintf(selected_row_requested_phenotype()$request_by_user) # sprintf(paste("BPeter1@ITS.JNJ.com"))
    cc <- sprintf(paste(paste(session$user, Sys.getenv("company_common_email"), sep = "")))
    subject <- paste("Your Phenotype Request has been APPROVED: ", selected_row_requested_phenotype()$phenotyperequest_id, " - ", selected_row_requested_phenotype()$phenotype_name, sep = "")
    body <- paste("Hello,
       Great News! Your request to add a new Phenotype has been approved by [ ", session$user, " ]

Your Phenotype has been added to the Library and can now be viewed from the homepage of the Phenotype Library App.

Comments from Approver: ", input$approveComments, "

Thank you,
Electra - Phenotype Library App
", sep = "")

    logger::log_info("Sending Email...")

    sendmailR::sendmail(from, to, subject, body, cc, control = list(smtpServer = Sys.getenv("servidor_SMTP")), attach.files = )

    logger::log_success("Email sent")
    Sys.sleep(5L)

    grid_requestedphenotype_page_df <<- rv()

    refresh()
  })

  observeEvent(input$rejectPhenotype, {
    # shinyjs::disable("approvePhenotype")
    shinyjs::disable("rejectPhenotype")
    # shinyjs::disable("requestMoreInfoPhenotype")

    print("add reject code here")

    # Collect Comments from User and save to Database
    showModal(modalDialog(
      tags$h2("Please enter comments for your Rejection"),
      textInput("rejectionComments", "Comments"),
      footer = tagList(
        actionButton("submitRejection", "Submit"),
        modalButton("cancel")
      )
    ))
  })

  # record the rejection and comments to db
  observeEvent(input$submitRejection, {
    removeModal()
    # Record the data to requested phenotype response tabe in db
    print("recording approval to database PHENOTYPE_LIBRARY.requested_phenotype_response...")
    print(input$rejectionComments)

    # Run insert query to add request to the db table
    save_request_phenotype_response <-
      paste(paste("INSERT INTO PHENOTYPE_LIBRARY.requested_phenotype_response(  phenotyperequest_id,  status,  reviewedby,  review_date,  review_time, comments) VALUES('", selected_row_requested_phenotype()$phenotyperequest_id, "','REJECTED','", session$user, "','", Sys.Date(), "','", Sys.time(), "','", str_replace(input$rejectionComments, "'", "''"), "' );", sep = ""),
        sep = ""
      )

    print("Save Response Requested Phenotype to Database")
    print(save_request_phenotype_response)

    execute_command(save_request_phenotype_response)

    # send email to requestor

    from <- sprintf(Sys.getenv("app_email")) # the sender’s name is an optional value
    to <- sprintf(selected_row_requested_phenotype()$request_by_user) # sprintf(paste("BPeter1@ITS.JNJ.com"))
    cc <- sprintf(paste(paste(session$user, Sys.getenv("company_common_email"), sep = "")))
    subject <- paste("Your Phenotype Request has been REJECTED: ", selected_row_requested_phenotype()$phenotyperequest_id, " - ", selected_row_requested_phenotype()$phenotype_name, sep = "")
    body <- paste("Hello,
       Unfortunately your request to add a new Phenotype has been rejected by [ ", session$user, " ]

You can review the comments from the approver and submit a new request.

Comments from Approver: ", input$rejectionComments, "

Thank you,
Electra - Phenotype Library App
", sep = "")

    logger::log_info("Sending Email...")

    sendmailR::sendmail(from, to, subject, body, cc, control = list(smtpServer = Sys.getenv("servidor_SMTP")), attach.files = )

    logger::log_success("Email sent")
    Sys.sleep(5L)
    refresh()
  })

  observeEvent(input$requestMoreInfoPhenotype, {
    shinyjs::disable("requestMoreInfoPhenotype")

    print("add request more info code here")

    # Collect Comments from User and save to Database
    showModal(modalDialog(
      tags$h2("Please enter what additional information is required"),
      textInput("requestMoreInfoComments", "Comments"),
      footer = tagList(
        actionButton("submitRequestMoreInfo", "Submit"),
        modalButton("cancel")
      )
    ))
  })

  # record the rejection and comments to db
  observeEvent(input$submitRequestMoreInfo, {
    removeModal()
    # Record the data to requested phenotype response tabe in db
    print("recording approval to database PHENOTYPE_LIBRARY.requested_phenotype_response...")
    print(input$requestMoreInfoComments)

    # Run insert query to add request to the db table
    save_request_phenotype_response <-
      paste(paste("INSERT INTO PHENOTYPE_LIBRARY.requested_phenotype_response(  phenotyperequest_id,  status,  reviewedby,  review_date,  review_time, comments) VALUES('", selected_row_requested_phenotype()$phenotyperequest_id, "','MORE INFO NEEDED','", session$user, "','", Sys.Date(), "','", Sys.time(), "','", str_replace(input$requestMoreInfoComments, "'", "''"), "' );", sep = ""),
        sep = ""
      )

    print("Save Response Requested Phenotype to Database")
    print(save_request_phenotype_response)

    execute_command(save_request_phenotype_response)

    # send email to requestor

    from <- sprintf(Sys.getenv("app_email")) # the sender’s name is an optional value
    to <- sprintf(selected_row_requested_phenotype()$request_by_user) 
    cc <- sprintf(paste(paste(session$user, Sys.getenv("company_common_email"), sep = "")))
    subject <- paste("Your Phenotype Request NEEDS MORE INFO: ", selected_row_requested_phenotype()$phenotyperequest_id, " - ", selected_row_requested_phenotype()$phenotype_name, sep = "")
    body <- paste("Hello,
       Unfortunately your request to add a new Phenotype has not been approved and the approver, [ ", session$user, " ] , is requesting additional information from you.

You can review the comments from the approver and submit a new request.

Comments from Approver: ", input$requestMoreInfoComments, "

Thank you,
Electra - Phenotype Library App
", sep = "")

    logger::log_info("Sending Email...")

    sendmailR::sendmail(from, to, subject, body, cc, control = list(smtpServer = Sys.getenv("servidor_SMTP")), attach.files = )
    logger::log_success("Email sent")
    Sys.sleep(5L)
    refresh()
  })

  input_file <- reactive({
    if (is.null(input$cohortDef_csvFile)) {
      return("")
    }

    # actually read the file
    read.csv(file = input$cohortDef_csvFile$datapath)
  })

  output$csvFileInputTable <- DT::renderDT({
    # render only if there is data available
    req(input_file())

    data <- input_file()

    data
  })

  mod_eval_cohort_output <- mod_evaluate_cohort_server("evaluate_cohort",
    databases_csv = "PL_App_Databases.csv",
    session_user = session$user,
    atlas_password = reactive(input$atlas_password),
    atlas_user_name = reactive(input$atlas_user_name),
    upload_to_s3 = TRUE
  )

  observeEvent(mod_eval_cohort_output(), {
    out <- req(mod_eval_cohort_output())
    # logger::log_info("Output from mod_eval_cohort_output is: ", toString(dput(out), Inf))
    if (is.null(out)) {
      logger::log_error("Output from mod_eval_cohort_output is NULL")
      return()
    } else if (isTRUE(out[[1]]$ExistingCohortBit)) {
      x <- out[[1]]
    } else if (!is.null(out[[input$dataBase]])) {
      x <- out[[input$dataBase]]
    } else {
      x <- out[[1]]
    }
    logger::log_info("(server.R) Updating sqliteDbPath with output from mod_eval_cohort_output: ", x$sqliteDbPath)

    checkmate::expect_names(names(x), must.include = c("sqliteDbPath", "cohort_id", "connectionHandler", "dataSource", "shinySettings"))
    checkmate::assert_file_exists(x$sqliteDbPath)
    checkmate::assert_true(!is.null(x$cohort_id))
    checkmate::assert_true(!is.null(x$connectionHandler))
    checkmate::assert_true(!is.null(x$dataSource))
    checkmate::assert_true(!is.null(x$shinySettings))

    sqliteDbPath(x$sqliteDbPath)

    logger::log_info("(server.R) Initiating module: OhdsiShinyModules::cohortDiagnosticsServer")
    cdModule <- OhdsiShinyModules::cohortDiagnosticsServer(
      id = "DiagnosticsExplorer",
      connectionHandler = x$connectionHandler,
      dataSource = x$dataSource,
      resultDatabaseSettings = x$shinySettings
    )
  })

  observe({
    co_id <- sub('.*C(\\d+):.*', '\\1', req(input$`DiagnosticsExplorer-targetCohort`))

    logger::log_info("(server.R) Selected Cohort ID: ", toString(co_id, Inf))
    cohort_id(co_id)
  })

  selected_db_code <- reactive({
    db <- req(input$`DiagnosticsExplorer-database`)
    logger::log_info("(server.R) Selected database: ", db)
    db
  })

  observe({
    databases <- req(input$dataBase)
    logger::log_info("(server.R) Selected database: ", input$dataBase)
    selected_dbs_df(
      PL_app_dbs %>%
        dplyr::filter(Name %in% databases)
    )
  })

  observeEvent(selected_db_code(), {
    selected_dbs_df(
      PL_app_dbs %>%
        dplyr::filter(db_code %in% selected_db_code())
    )
  })

  home_page_selected_cohort <- reactive({
    req(input$datatable_rows_selected)
    home_page_df()[input$datatable_rows_selected, ]
  })

  cohort_id <- reactiveVal()

  observeEvent(home_page_selected_cohort(), {
    cohort_id(req(home_page_selected_cohort())$`Cohort ID`)
  })

  observeEvent(selected_row(), {
    cohort_id(req(selected_row()$jnj_cohort_definition_id))
  })

  observeEvent(cohort_id(), {
    cohort_id <- req(cohort_id())
    get_cohort_definition_json(cohort_id = as.integer(cohort_id), remove_json_dir = !interactive())
  })

  list_tables <- eventReactive(list(sqliteDbPath(), cohort_id(), selected_db_code()), {

    sql_db_path <- req(sqliteDbPath())
    req(file.exists(sql_db_path))
    cohort_id <- req(cohort_id())
    selected_db_code <- req(selected_db_code())

    connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = sql_db_path)
    conn <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(conn))

    list_tables <- list()

    table_names <- get_correlation_analysis_output_table_names(cohort_id) %>%
      intersect(DBI::dbListTables(conn))

    if (length(table_names) == 0) {
      logger::log_warn("No correlations tables found in sql_db_path")
      return(NULL)
    }

    for (table in table_names) {
      if (DBI::dbExistsTable(conn, table)) {
        req_cols_exist <- all(c("database_id", "cohort_id") %in% names(DBI::dbGetQuery(conn, glue::glue("SELECT * FROM {table} LIMIT 1;"))))
        if (!req_cols_exist) {
          logger::log_warn("Required columns database_id, cohort_id do not exist in table from SQLite: ", table)
          list_tables[[table]] <- FALSE
        } else {
          qry <- glue::glue("SELECT * FROM {table} WHERE cohort_id = {cohort_id} AND database_id = '{selected_db_code}';")
          logger::log_info(glue::glue("Pulling data from database: '{qry}'"))
          list_tables[[table]] <- DBI::dbGetQuery(conn, qry) %>%
            dplyr::rename_with(~ gsub("^v([0-9]+)$", "V\\1", .x))
        }
      } else {
        logger::log_warn(glue::glue("Table '{table}' does not exist in {sql_db_path}"))
        list_tables[[table]] <- FALSE
      }
    }

    table <- "condition_occurrence_correlations"


    list_tables[[table]] %>%
      names() %>%
      sapply(function(col) {
        vals <- list_tables[[table]][[col]] %>% unique()

        if (!isTruthy(vals)) {
          err_msg <- glue::glue("Column '{col}' in table '{table}' has empty values : ", toString(vals),
                                ". Run forced cohort evaluation and try again.")
          logger::log_error(err_msg)
          toast(err_msg, type = "error", timer = 60L * 1000L)
          req(FALSE)
        }
      })

    list_tables
  })

  conceptsCorrelationServer("conceptsCorrelation", list_tables)

  mod_prev_changes_out <- prevalenceChangesServer("prev_changes",
                          cohort_id = cohort_id,
                          selected_dbs_df = selected_dbs_df,
                          sqlite_db_path = sqliteDbPath)

  mod_time_dep_out <- timeDependentServer("time_dep",
                                          cohort_id = cohort_id,
                                          selected_dbs_df = selected_dbs_df,
                                          sqlite_db_path = sqliteDbPath)
})
