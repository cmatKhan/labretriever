#' Send a dataframe to a server endpoint
#'
#' This function sends a dataframe to a server endpoint using the POST method
#'   and the JSON format.
#'
#' @inheritParams get_pagination_info
#'
#' @param df A dataframe to send to the server endpoint.
#' @param strict Ensure that writable columns in the database table are
#'   present in input df. the Defaults to TRUE.
#' @param update Set to TRUE if you are updating a table. This will turn off
#'   the strict check -- only the 'id' column is required.
#'   Additional columns will be those updated, according to the 'id'. This
#'   will be used to append to the url
#'
#' @return Nothing is returned explicitly; the function logs the HTTP
#'   response status and message instead.
#'
#' @importFrom jsonlite toJSON
#' @importFrom httr POST add_headers status_code content
#' @importFrom futile.logger flog.debug flog.error
#'
#' @examples
#' \dontrun{
#' labretriever::send(df, "http://example.com/api/myendpoint", "my_token")
#' }
#' @export
send <- function(df, url, token, strict = TRUE, update = FALSE) {
  # get readable fields in the table
  table_fields <- labretriever::get_field_info(url, token)

  # get list of fields which the user is responsible for
  user_write_fields <- setdiff(
    table_fields$writable,
    table_fields$automatically_generated
  )

  if (update) {
    if (!"id" %in% colnames(df) |
      length(setdiff(colnames(df)[colnames(df) != 'id'],
                     user_write_fields) > 0)) {
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

    # if no error, post the update and catch the response
    response <- post_update(df, url, token)
  } else if (strict) {
    setdiff_df_cols_user_write_fields <-
      setdiff(user_write_fields, colnames(df))

    if (length(setdiff_df_cols_user_write_fields) > 0) {
      tryCatch(stop("ValueError"), error = function(e) {
        futile.logger::flog.error(paste(
          "The following columns are missing from the dataframe,",
          "but necessary in the served table:",
          paste(setdiff_df_cols_user_write_fields, collapse = ", "),
          sep = " "
        ))
      })
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

      # Convert the dataframe to JSON format
      post_body <- jsonlite::toJSON(df, auto_unbox = TRUE)

      # Send the POST request and capture the response
      response <- httr::POST(
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
  } # end strict

    # Construct the HTTP response message using the function name and
    # response object
    response_msg <- labretriever::construct_basic_response_msg(
      as.character(match.call()[[1]]),
      response
    )
    # Log the response status and message based on the HTTP status category
    if (httr::http_status(response)$category == "Success") {
      futile.logger::flog.info(response_msg)
    } else {
      futile.logger::flog.error(
        tryCatch(stop("HTTP Error"), error = function(e) {
          labretriever::extend_msg_error(response_msg, response, url)
        },
        finally = stop(paste0('HTTP Error: ',
                              labretriever::extend_msg_error(response_msg,
                                                             response, url))))
      )
    }
}

#' a helper for send() function
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
