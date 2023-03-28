#' Construct a basic HTTP response message
#'
#' This function constructs a basic HTTP response message by combining
#'   the name of the function that made the request and the HTTP status
#'   code returned by the server.
#'
#' @param func_name A character string representing the name of the
#'   function that made the request.
#' @param response An object of class `response` from the `httr` package.
#'
#' @return A character string representing the HTTP response message.
#'
#' @examples
#' \dontrun{
#' construct_basic_response_msg("get_data", response)
#' }
#'
#' @export
construct_basic_response_msg <- function(func_name, response) {
  # Combine the function name and HTTP status code into a message
  paste0(
    func_name, "()", " HTTP response: ",
    paste(names(httr::http_status(response)),
      httr::http_status(response),
      sep = " - ",
      collapse = ": "
    )
  )
}


#' Extend an error message with additional details
#'
#' This function takes an initial error message, an HTTP response object, and a URL, and extends the error message with additional details about the error.
#'
#' @param init_msg A character string representing the initial error message.
#' @param response An object of class `response` from the `httr` package.
#' @param url A character string representing the URL that was requested.
#'
#' @return A character string representing the extended error message.
#'
#' @examples
#' \dontrun{
#' extend_msg_error(
#'   construct_basic_response_msg("func", response),
#'   response, "https://example.com/data"
#' )
#' }
#'
#' @export
extend_msg_error <- function(init_msg, response, url) {
  # Combine the initial error message and the HTTP response
  # details into a message
  paste0(
    init_msg, ".\n",
    "\tDetails: ", httr::content(response),
    "\n\t", paste0("URL: ", url)
  )
}
