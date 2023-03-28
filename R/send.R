#' Send a dataframe to a server endpoint
#'
#' This function sends a dataframe to a server endpoint using the POST method
#'   and the JSON format.
#'
#' @inheritParams get_pagination_info
#'
#' @param df A dataframe to send to the server endpoint.
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
send <- function(df, url, token) {
  # get readable fields in the table
  table_fields <- labretriever::get_field_info(url, token)

  # get list of fields which the user is responsible for
  user_write_fields <- setdiff(
    table_fields$writable,
    table_fields$automatically_generated
  )

  # if setdiff_df_cols_user_write_fields is not empty, then there are
  # fields for which the user is responsible, but are not present in the df
  # TODO offer a 'strict' argument that checks only fields which do not have
  # a default -- will need to write this into the endpoint code, too
  setdiff_df_cols_user_write_fields <- setdiff(user_write_fields,colnames(df))

  if (length(setdiff_df_cols_user_write_fields) > 0) {
    futile.logger::flog.error(paste(
      "The following columns are missing from the dataframe,",
      "but necessary in the served table:",
      paste(setdiff_df_cols_user_write_fields, collapse = ", "),
      sep = " "
    ))
  } else {
    # check that there are no automatically generated fields in the df
    auto_gen_fields <- intersect(
      colnames(df),
      table_fields$automatically_generated
    )
    if (length(auto_gen_fields) != 0) {
      futile.logger::flog.warn(
        paste("removing the following columns",
          "because they are automatically generated:",
          paste(auto_gen_fields, collapse = ", "),
          sep = " "
        )
      )
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
        labretriever::extend_msg_error(response_msg, response, url)
      )
    }
  }
}
