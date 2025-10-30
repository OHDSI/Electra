# Start of Save Logic Functions-----------------------------------


#' table_exists
#'
#' Function to check if the table exists
#'
#' @param table_name
#'
#' @return
#' @export
#'
table_exists <- function(table_name) {
  query <- paste0("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '", table_name, "');")
  result <- run_query_RW(query)
  #  logger::log_info(paste("result", result))
  return(result)
}

#' Function to create a table based on the data frame structure
#'
#' @param table_name
#' @param data
#'
#' @returns
#' @export
#'
create_table <- function(table_name, data) {
  fields <- sapply(names(data), function(column) {
    dtype <- ifelse(is.numeric(data[[column]]),
                    "DOUBLE PRECISION",
                    ifelse(
                      is.integer(data[[column]]),
                      "INTEGER",
                      ifelse(
                        is.logical(data[[column]]),
                        "BOOLEAN",
                        ifelse(inherits(data[[column]], "Date"), "DATE", "TEXT")
                      )
                    ))
    paste0(column, " ", dtype)
  })
  fields_sql <- paste(fields, collapse = ", ")
  create_table_sql <- paste0("CREATE TABLE ",
                             paste0(tablePrefix, ".", table_name),
                             " (",
                             fields_sql,
                             ");")
  execute_command(create_table_sql)
}


#' get_table_schema
#'
#' Create query to retrieve existing table (Function to retrieve the existing table schema using run_query_RW from global.R)
#'
#' @param table_name
#'
#' @return
#' @export
#'
get_table_schema <- function(table_name) {
  # Construct the schema query
  schema_query <- paste0("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '", table_name, "';")

  # Debugging: Print the query
  # logger::log_info("Schema query: ", schema_query)

  # Run the query using run_query_RW
  schema <- run_query_RW(schema_query)

  # Debugging: Print the retrieved schema
  logger::log_info("Retrieved schema: ", toString(schema, Inf))

  return(schema)
}


#' coerce_data_types
#'
# Get column names and data type from database table (Function to coerce data types to match the database schema)
#'
#' @param table_name
#' @param data
#'
#' @return
#' @export
#'
coerce_data_types <- function(table_name, data) {
  # Retrieve the table schema
  schema <- get_table_schema(table_name)

  if (nrow(schema) == 0) {
    stop(paste("No schema information found for table:", table_name))
  }

  # Debugging: Print the column names of the data
  #  logger::log_info("Column names in data:")
  #  logger::log_info(colnames(data))

  for (i in 1:nrow(schema)) {
    column <- schema$column_name[i]
    type <- schema$data_type[i]

    # Debugging: Print the current column and its type
    #   logger::log_info("Processing column: ", column, " with type: ", type)

    if (column %in% colnames(data)) {
      if (type == "integer") {
        data[[column]] <- as.integer(data[[column]])
      } else if (type == "numeric" || type == "double precision" || type == "real") {
        data[[column]] <- as.numeric(data[[column]])
      } else if (type == "character varying" || type == "text") {
        data[[column]] <- as.character(data[[column]])
      } else if (type == "boolean") {
        data[[column]] <- as.logical(data[[column]])
      } else if (type == "date") {
        data[[column]] <- as.Date(data[[column]])
      }
      # Add other type coercions as needed
    } else {
      logger::log_info("Column ", column, " not found in data.")
    }
  }

  return(data)
}


#' filter_data_columns
#'
#' Function to filter data frame columns to match the database table columns
#'
#' @param table_name
#' @param data
#'
#' @return
#' @export
#'
#' @examples
filter_data_columns <- function(table_name, data) {
  schema <- get_table_schema(table_name)
  table_columns <- schema$column_name
  common_columns <- intersect(names(data), table_columns)
  filtered_data <- data[, common_columns, drop = FALSE]
  # filtered_data <- data %>% select(all_of(table_columns))
  #  logger::log_info(table_columns)
  #  logger::log_info(filtered_data)
  return(filtered_data)
}

#' Function to generate an SQL INSERT statement for a data frame
#'
#' @param table_name
#' @param data
#'
#' @return
#' @export
#'
generate_insert_query <- function(table_name, data) {
  tablePrefix <- "phenotype_library"
  columns <- paste(names(data), collapse = ", ")
  values <- apply(data, 1, function(row) {
    paste0("(", paste(sapply(row, function(x) {
      if (is.na(x)) {
        "NULL"
      } else if (is.character(x)) {
        paste0("'", gsub("'", "''", x), "'")
      } else {
        x
      }
    }), collapse = ", "), ")")
  })
  values <- paste(values, collapse = ", ")
  query <- paste0("INSERT INTO ", paste0(tablePrefix, ".", table_name), " (", columns, ") VALUES ", values, ";")
  # query <- paste0("INSERT INTO ", table_name, " (", columns, ") VALUES ", values, ";")
  return(query)
}


# Function to check if a cohort_id and database_id combination exists in a table
# combination_exists <- function(table_name, cohort_id, database_id) {
#   tablePrefix <- "phenotype_library"
#   query <- paste0(
#     "SELECT EXISTS (",
#     "SELECT 1 FROM ", paste0(tablePrefix,".",table_name),
#     " WHERE cohort_id = '", cohort_id, "' AND database_id = '", database_id, "'",
#     ");"
#   )
#   result <- run_query_RW(query)
#   return(result)
# }
#' Function to check if the combination of cohort ID and database ID exists
#'
#' @param schema_name
#' @param cohort_table
#' @param cohort_id
#' @param database_id
#'
#' @return
#' @export
#'
combination_exists <- function(schema_name, cohort_table, cohort_id, database_id) {
  checkmate::assert_string(schema_name, min.chars = 3)
  checkmate::assert_string(cohort_table, min.chars = 3)
  checkmate::assert_string(cohort_id, min.chars = 3)
  checkmate::assert_vector(database_id, min.len = 1)

  logger::log_info("(combination_exists) Checking if combination_exists in PostgreSQL...")
  # Split cohort_id by underscores and format it for SQL IN clause
  cohort_ids <- strsplit(cohort_id, "_")[[1]]
  formatted_cohort_ids <- paste0("'", paste(cohort_ids, collapse = "','"), "'")
  database_id <- strsplit(database_id, ",")[[1]]
  formatted_database_ids <- paste0("'", paste(database_id, collapse = "','"), "'")
  logger::log_info(formatted_cohort_ids)
  logger::log_info(formatted_database_ids)
  # Update query to use IN clause
  query <- paste0(
    "SELECT cohort_id, database_id FROM ", schema_name, ".", cohort_table,
    " WHERE cohort_id IN (", formatted_cohort_ids,
    ") AND database_id IN (", formatted_database_ids, ");"
  )
  logger::log_info("(combination_exists) Querying db: ", query)
  result <- run_query_RW(query)

  if (all(cohort_ids %in% result$cohort_id) && all(database_id %in% result$database_id)) {
    bool <- "t"
  } else {
    bool <- "f"
  }

  # Debugging: Print the result
  logger::log_success("(combination_exists) Result of combination EXISTS query: ", toString(bool))

  # Return the logical value
  return(bool)
}



#' Function to upload multiple CSV files from a zip archive to existing database tables
#'
#' @param zip_file_path
#' @param CohortID
#' @param selectedDatabase
#' @param overwrite
#' @param append
#'
#' @return
#' @export
#'
upload_csvs_from_zip_to_db <- function(zip_file_path,
                                       CohortID,
                                       selectedDatabase,
                                       overwrite = FALSE,
                                       append = TRUE,
                                       tablePrefix = "phenotype_library") {

  # Create a temporary directory to extract the zip files
  temp_dir <- tempdir()
  unzip(zip_file_path, exdir = temp_dir)

  # List all CSV files in the temporary directory
  csv_files <- list.files(temp_dir, pattern = "*.csv", full.names = TRUE)

  # Iterate over each cohort id by splitting into integer
  cohort_id_list <- numeric()

  if (grepl("_", CohortID)) {
    cohort_id_list <- as.integer(unlist(strsplit(as.character(CohortID), "_")))
  }  else if (grepl(",", CohortID)) {
    cohort_id_list <- as.integer(unlist(strsplit(as.character(CohortID), ",")))
  } else if (grepl("", CohortID)) {
    cohort_id_list <- as.integer(unlist(strsplit(as.character(CohortID), ",")))
  } else {
    cohort_id_list <- as.integer(CohortID)
  }

  logger::log_info("cohort_id_list: ", toString(cohort_id_list, Inf))


  for (co in cohort_id_list) {
    logger::log_info(paste("current_cohort_id :", co))

    # Iterate over each CSV file and upload it to the database
    for (csv_file in csv_files) {
      # Read the CSV file
      data <- readr::read_csv(csv_file, show_col_types = FALSE)

      # Columns in PostgreSQL can technically be uppercase, but only if you enclose the column name in double quotes when creating and referencing it. By default, PostgreSQL converts unquoted column names to lowercase, so using uppercase or mixed-case identifiers without double quotes will result in them being treated as lowercase.
      data <- data %>% dplyr::rename_all(tolower)


      # Print the first few rows of the data for debugging
        logger::log_info("First few rows of the data: ")
        print(head(data))

      # Update cohort id and database id for missing column in CSV
      if (!"cohort_id" %in% colnames(data)) {
        data$cohort_id <- co
      }
      if (!"database_id" %in% colnames(data)) {
        data$database_id <- selectedDatabase
      }

      checkmate::assert_names(names(data), must.include = c("cohort_id", "database_id"))

      # If csv doesn't have any records then skip
      if (nrow(data) == 0) {
        logger::log_info("CSV file ", csv_file, " has no records. Skipping.")

        next
      }

      # Create a table name based on the file name
      # table_name <- paste0(table_prefix, ".", tools::file_path_sans_ext(basename(csv_file)))
      table_name <- paste0(tools::file_path_sans_ext(basename(csv_file)))
      logger::log_info("Raw table name: ", table_name)

      if (table_name == "cohort_inc_result") {
        table_name <- "cohort_inc_results"
      } else if (table_name == "concept_synonym") {
        table_name <- "concept_synonyms"
      } else if (table_name == "executionTimes") {
        table_name <- "executiontimes"
      } else if (table_name == "orphan_concept") {
        table_name <- "orphan_concepts"
      } else {
        table_name
      }
      logger::log_info("Table name after applying logic: ", table_name)

      is_correlation_analysis_table <- table_name %in% get_correlation_analysis_output_table_names()
      logger::log_info("is_correlation_analysis_table: ", is_correlation_analysis_table)

      # Check if table exists or create a new table
      if (isTRUE(table_exists(table_name) == "f")) {
         create_table(table_name, data)
        logger::log_info("Table ", table_name, "created.")
      } else {

        # Check if the combination of cohort_id and database_id already exists in the table
        # unless table comes from correlation analysis, so then it will be alwayes inserted
        comb_exists <- combination_exists(schema_name = tablePrefix, cohort_table = table_name, cohort_id = as.character(co), database_id = selectedDatabase) == "t"
        if (!is_correlation_analysis_table && (!overwrite && comb_exists)) {
          logger::log_info("Combination of cohort_id ", co, " and database_id ", selectedDatabase, " already exists in table ", table_name, ". Skipping.")
          next
        }

        conn <-  DatabaseConnector::connect(get_postgresql_connection_details())
        on.exit(DBI::dbDisconnect(conn))

        if (DBI::dbExistsTable(conn, table_name)) {
          # Filter data to include only columns that exist in the database table
          data <- filter_data_columns(table_name, data)
          # Coerce data types to match the database schema
          data <- coerce_data_types(table_name, data)

          # Print the coerced data types for debugging
          logger::log_info("Coerced data types:")
          # logger::log_info(str(data))

          tryCatch(
            {
              logger::log_info("Inserting table:", table_name)

              # Generate the insert command
              insert_query <- generate_insert_query(table_name, data)

              # logger::log_info(insert_query)
              execute_command(insert_query)

              #  dbWriteTable(con, table_name, data, append = append, row.names = FALSE)
              logger::log_info("Data from ", csv_file, " appended successfully to ", table_name, " table.")
            },
            error = function(e) {
              logger::log_info("Error appending data from ", csv_file, " to ", table_name, " : ", e$message)
            }
          )
        } else {
          logger::log_info(glue::glue("Table '{table_name}' does not exist. Creating..."))
          DBI::dbWriteTable(conn = conn, name = paste0("phenotype_library.", table_name), value = data)
        }
      }
    }
  }
}
# End of Save Logic Functions-----------------------------------
