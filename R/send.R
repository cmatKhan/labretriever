#' Send a dataframe to a server endpoint
#'
#' This function sends a dataframe to a server endpoint using the POST method
#'   and the JSON format.
#'
#'
#' @param df A dataframe to send to the server endpoint.
#' @param url The base URL of the endpoint, such as
#'   "http://127.0.0.1:8000/api/v1/genes/".
#' @param token Your API authentication token, if required.
#' @param update Set to TRUE if you are updating a table. This will turn off
#'   the strict check and ignore the the async or csv endpoints. Only the 'id'
#'   column is required. Additional columns will be those updated, according
#'   to the 'id'. This will be used to append to the url
#' @param strict Ensure that writable columns in the database table are
#'   present in input df. the Defaults to TRUE.
#' @param async Boolean. Default FALSE. Post the data to an
#'   asynchronous endpoint
#' @param csv Boolean. Default FALSE. Post the data as a csv file
#' @param csv_postgres Boolean. Default FALSE. This sends a csv file to an
#'   endpoint set up to create bulk records using the postgresql COPY
#'   method. This is faster for larger data, but will not work on other
#'   database backends
#'
#' @return Nothing is returned explicitly; the function logs the HTTP
#'   response status and message instead.
#'
#' @importFrom jsonlite toJSON
#' @importFrom httr POST add_headers status_code content upload_file
#' @importFrom futile.logger flog.debug flog.error
#'
#' @examples
#' \dontrun{
#' labretriever::send(df, "http://example.com/api/myendpoint", "my_token")
#' }
#' @export
send <- function(df, url, token,
                 update = FALSE,
                 strict = TRUE,
                 async = FALSE,
                 csv = FALSE,
                 csv_postgres = FALSE) {

  if (async & csv) {
    stop(paste0("async and csv cannot both be ",
                "TRUE -- choose one"))
  }

  # get readable fields in the table
  table_fields <- labretriever::get_field_info(url, token)

  # get list of fields which the user is responsible for
  user_write_fields <- setdiff(
    table_fields$writable,
    table_fields$automatically_generated
  )

  # First check if the update flag is set -- if this is set, then the
  # http request is made in this block. The function picks up again at the
  # logging stage (strct and the endpoints are not evaluated, in other
  # words)
  if (update) {
    if (!"id" %in% colnames(df) |
        length(setdiff(
          colnames(df)[colnames(df) != "id"],
          user_write_fields
        ) > 0)) {
      tryCatch(stop("ValueError"), error = function(e) {
        futile.logger::flog.error(
          paste("A field named either `id`",
                "must be in the colnames for an update and",
                "the update columns must be user writable",
                "columns:",
                paste(user_write_fields,
                      collapse = ", "
                ),
                sep = " "
          )
        )
      })
    }
    # note: still in update block
    # if no error, post the update and catch the response
    response <- post_update(df, url, token)

    # if update is false, then check whether strict is TRUE. If it is,
    # do a strict check of the columns
  } else if (strict) {
    setdiff_df_cols_user_write_fields <-
      setdiff(user_write_fields, colnames(df))
    # if there are missing columns throw an error
    # TODO change this to a stop() statement -- catch in application
    if (length(setdiff_df_cols_user_write_fields) > 0) {
      tryCatch(stop("ValueError"), error = function(e) {
        futile.logger::flog.error(paste(
          "The following columns are missing from the dataframe,",
          "but necessary in the served table:",
          paste(setdiff_df_cols_user_write_fields, collapse = ", "),
          sep = " "
        ))
      })
      # if there are no missing fields, check to make sure that the
      # auto-generated fields are not present. Remove them if they are
    } else {
      # check that there are no automatically generated fields in the df
      auto_gen_fields <- intersect(
        colnames(df),
        table_fields$automatically_generated
      )
      # remove fields that are not user write-able and warn
      if (length(auto_gen_fields) != 0) {
        futile.logger::flog.warn(
          paste("removing the following columns",
                "because they are automatically generated:",
                paste(auto_gen_fields, collapse = ", "),
                sep = " "
          )
        )
        # update the input dataframe
        df <- df[, !colnames(df) %in% auto_gen_fields]
      }
      # choose the appropriate post function
      if(async){
        response = post_async(df, url, token)
      } else if(csv){
        response = post_csv(df, url, token)
        # else, assume that the post should be sent as a json
      } else if (csv_postgres){
        response = post_postgres_csv(df, url, token)
      } else{
        response = post_json(df, url, token)
      }
    }
  } # end strict

  # Construct the HTTP response message using the function name and
  # response object
  response_msg <- labretriever::construct_basic_response_msg(
    as.character(match.call()[[1]]),
    response
  )
  # Log the response status and message based on the HTTP status category
  if (httr::http_status(response)$category == "Success") {
    futile.logger::flog.info(paste0('labretriever::send() successful!: ',
                                    httr::http_status(response)))
  } else {
    futile.logger::flog.error(
      tryCatch(stop("HTTP Error"),
               error = function(e) {
                 labretriever::extend_msg_error(response_msg, response, url)
               },
               finally = stop(paste0(
                 "HTTP Error: ",
                 labretriever::extend_msg_error(
                   response_msg,
                   response, url
                 )
               ))
      )
    )
  }
  response
}


#' a helper for send() function for json data
#' @param df a dataframe with the column `id`
#' @param url the url to the table endpoint -- the `id` is added in
#'   this function
#' @param token authorization token
#'
#' @importFrom dplyr select
#'
post_json <- function(df, url, token) {
  # Send the POST request and capture the response

  post_body <- jsonlite::toJSON(df, auto_unbox = TRUE)

  httr::POST(
    url = url,
    httr::add_headers(
      Authorization =
        paste("token", token, sep = " ")
    ),
    httr::content_type("application/json"),
    body = post_body,
    encode = "json"
  )
}


#' a helper for send() function for csv data
#' @param df a dataframe with the column `id`
#' @param url the url to the table endpoint -- the `id` is added in
#'   this function
#' @param token authorization token
#' @param csv_endpoint an additional string to append to the
#'   url which will reach an endpoint for a given view that is configured
#'   for bulk uploads of csv files, eg 'upload-csv/'. NOTE this also sets
#'   the upload data type to csv currently
#'
#' @importFrom dplyr select
#'
post_csv <- function(df, url, token, csv_endpoint = "upload-csv/") {
  # create a tmp_file in case this is a csv_endpoint
  # This will be removed at the end of the function
  tmp_file <- tempfile()
  # update the url
  # TODO move this to create_url and add to database_info
  url <- paste0(url, csv_endpoint)
  # set the content type
  upload_datatype <- httr::content_type("text/csv")
  encoding <- "multipart"
  write.csv(df, file = tmp_file, row.names = FALSE)
  post_body <- list(csv_file = httr::upload_file(tmp_file,
                                                 type = "text/csv"
  ))

  # Send the POST request and capture the response
  response <- httr::POST(
    url = url,
    httr::add_headers(
      Authorization =
        paste("token", token, sep = " ")
    ),
    body = post_body,
    encode = "multipart"
  )

  unlink(tmp_file)

  return(response)
}


#' a helper for send() function for csv data
#' @param df a dataframe with the column `id`
#' @param url the url to the table endpoint -- the `id` is added in
#'   this function
#' @param token authorization token
#' @param csv_postgres_endpoint an additional string to append to the
#'   url which will reach an endpoint for a given view that is configured
#'   for bulk uploads of csv files, eg 'upload-csv/'. NOTE this also sets
#'   the upload data type to csv currently
#'
#' @importFrom dplyr select
#'
post_postgres_csv <- function(df, url, token,
                              csv_postgres_endpoint = "upload-csv-postgres/") {
  # create a tmp_file in case this is a csv_endpoint
  # This will be removed at the end of the function
  tmp_file <- tempfile()
  # update the url
  # TODO move this to create_url and add to database_info
  url <- paste0(url, csv_postgres_endpoint)
  # set the content type
  upload_datatype <- httr::content_type("text/csv")
  encoding <- "multipart"
  write.csv(df, file = tmp_file, row.names = FALSE)
  post_body <- list(csv_file = httr::upload_file(tmp_file,
                                                 type = "text/csv"
  ))

  # Send the POST request and capture the response
  response <- httr::POST(
    url = url,
    httr::add_headers(
      Authorization =
        paste("token", token, sep = " ")
    ),
    body = post_body,
    encode = "multipart"
  )

  unlink(tmp_file)

  return(response)
}

#' a helper for send() function to update records
#' @param df a dataframe with the column `id`
#' @param url the url to the table endpoint -- the `id` is added in
#'   this function
#' @param token authorization token
#'
#' @importFrom dplyr select
#'
post_update <- function(df, url, token) {
  # Convert the dataframe to JSON format
  url <- paste0(file.path(gsub("/$", "", url), gsub("^/", "", df$id)), '/')
  df <- df %>% dplyr::select(-id)

  post_body <- jsonlite::toJSON(df, auto_unbox = TRUE)

  # Send the POST request and capture the response
  httr::PUT(
    url = url,
    httr::add_headers(
      Authorization =
        paste("token", token, sep = " ")
    ),
    httr::content_type("application/json"),
    body = post_body,
    encode = "json"
  )
}

