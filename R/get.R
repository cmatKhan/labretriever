construct_basic_response_msg = function(func_name, response){

  paste0(func_name, "()", " HTTP response: ",
         paste(names(httr::http_status(response)),
               httr::http_status(response),
               sep = " - ",
               collapse = ": "))
}

extend_msg_error = function(init_msg, response, url){
  paste0(init_msg, ".\n",
         "\tDetails: ", httr::content(response),
         "\n\t", paste0('pagination url: ', url))
}

#' Get pagination information from a REST_FRAMEWORK endpoint
#'
#' This function retrieves the pagination information for a given REST_FRAMEWORK
#' endpoint. It returns a list with the default page size and page size limit.
#'
#' @param pagination_url The URL of the REST_FRAMEWORK endpoint for
#'   which to retrieve the pagination information
#' @param token The authentication token to use when accessing the endpoint
#'
#' @return A list with the default page size and page size limit
#'
#' @seealso \code{\link{get_user_auth_token}}
#'
#' @examples
#' \dontrun{
#' pagination_url <- "http://127.0.0.1:8000/api/v1/chrmap/pagination_info/"
#' token <- get_user_auth_token(
#'   "http://example.com/api/get_token",
#'   "user", "password"
#' )
#'
#' # Retrieve the pagination information
#' pagination_info <- get_pagination_info(pagination_url, token)
#' }
#'
#' @importFrom httr GET add_headers http_status status_code content
#' @importFrom futile.logger flog.debug flog.error
#'
#' @export
get_pagination_info <- function(pagination_url, token) {
  # Retrieve the page size limit from the REST_FRAMEWORK settings
  response <- httr::GET(
    pagination_url,
    httr::add_headers(
      "Authorization" = paste("token", token, sep = " "),
      "Content-Type" = "application/json"
    )
  )

  response_msg = construct_basic_response_msg(
    as.character(match.call()[[1]]),
    response)

  if (!httr::status_code(response) %in% c("200", "201")) {
    futile.logger::flog.error(
      extend_msg_error(response_msg,
                       response,
                       pagination_url))
  } else {
    futile.logger::flog.debug(response_msg)
    # return a list, eg list(default_page_size = 10, page_size_limit = 10)
    httr::content(response)
  }
}

#' Retrieve the total number of records from a count endpoint
#'
#' This function retrieves the total number of records from a count
#'   endpoint and returns it as a numeric value.
#'
#' @param count_url The URL of the count endpoint.
#'
#' @return The total number of records as a numeric value.
#'
#' @examples
#' \dontrun{
#' # Get the total number of records from a count endpoint
#' count_url <- "https://example.com/api/count"
#' token <- "my_token"
#' total_records <- get_total_records(count_url, token)
#' }
#' @seealso \code{\link{get_pagination_info}}, \code{\link{get_user_auth_token}}
#'
#' @inheritParams get_pagination_info
#' @inheritParams get_user_auth_token
#'
#' @importFrom httr GET add_headers status_code content
#' @importFrom futile.logger flog.debug flog.error
#'
#' @export
get_total_records <- function(count_url, token) {
  response <- httr::GET(
    count_url,
    httr::add_headers(
      "Authorization" = paste("token", token, sep = " "),
      "Content-Type" = "application/json"
    )
  )

  response_msg = construct_basic_response_msg(
    as.character(match.call()[[1]]),
    response)

  if (!httr::status_code(response) %in% c("200", "201")) {
    futile.logger::flog.error(extend_msg_error(
      response_msg,
      response,
      count_url))
  } else {
    futile.logger::flog.debug(response_msg)
    # return the total number of records
    httr::content(response)
  }
}


#' Create a function to return a function to get paginated results
#'
#' This function returns a function that can be called to make a GET request to
#' retrieve data from a paginated API endpoint.
#'
#' @param url The URL of the endpoint to retrieve data from
#' @param token The authentication token to use for the GET request
#' @param page_size The number of records to retrieve per page
#'
#' @return A function that can be called with a start index to retrieve
#'   a page of results
#'
#' @examples
#' \dontrun{
#' get_by_page <- get_table_by_page(
#'   "http://example.com/api/endpoint",
#'   "my_auth_token", 10
#' )
#'
#' get_by_page(1)
#' }
#'
#' @importFrom httr GET add_headers status_code content
#' @importFrom futile.logger flog.debug flog.error
#'
#' @export
get_table_by_page <- function(url, token, page_size) {
  # create a persistent function -- like a class -- which has url, token and
  # page_size set. The user can then call the function that is returned out
  # of this function with different start_indexes
  inner <- function(start_index) {
    curr_url <- paste0(
      url,
      "?pageSize=",
      page_size,
      "&startIndex=",
      start_index
    )

    response <- httr::GET(
      curr_url,
      httr::add_headers(
        "Authorization" = paste("token", token, sep = " "),
        "Content-Type" = "application/json"
      )
    )

    response_msg = construct_basic_response_msg(
      as.character(match.call()[[1]]),
      response)

    if (!httr::status_code(response) %in% c("200", "201")) {
      futile.logger::flog.error(
        extend_msg_error(response_msg,
                         response,
                         curr_url))
    } else {
      futile.logger::flog.debug(response_msg)
      # return the content of the response
      httr::content(response, "text", encoding = "UTF-8")
    }
  }

  # return the inner function to the caller
  inner
}

#' Get data from a REST API with pagination
#'
#' This function retrieves data from a REST API that uses pagination.
#'
#' @inheritParams get_pagination_info
#'
#' @param url URL of the REST API
#' @param pagination_endpoint Endpoint for pagination
#' @param count_endpoint Endpoint for counting total records
#'
#' @return A tibble containing the data retrieved from the API
#'
#' @importFrom httr GET content status_code http_status
#' @importFrom jsonlite fromJSON
#' @importFrom tibble as_tibble
#' @importFrom futile.logger flog.error
#'
#' @import foreach
#'
#' @examples
#' \dontrun{
#' get_data("https://example.com/api", "1234")
#' }
#'
#' @seealso \code{\link{get_pagination_info}}, \code{\link{get_total_records}}
#'
#' @export
get_data <- function(
    url,
    token,
    pagination_endpoint = database_info$endpoints$pagination_info,
    count_endpoint = database_info$endpoints$row_count) {

  # Get pagination URL and info
  pagination_url <- file.path(gsub("/$", "", url), pagination_endpoint)
  pagination_info <- get_pagination_info(pagination_url, token)

  # Get total number of records
  count_url <- file.path(gsub("/$", "", url), count_endpoint)
  total_records <- get_total_records(count_url, token)

  tryCatch({
    # Calculate number of pages based on page size
    page_size <- pagination_info$page_size_limit
    total_pages <- ceiling(total_records$count / page_size)

    # Create an empty list to store results
    results <- vector(mode = "list", length = total_pages)

    # Create a function to get table by page
    table_by_page <- get_table_by_page(url, token, page_size)

    # Download pages in parallel
    foreach::foreach(
      i = 1:total_pages,
      .combine = "rbind"
    ) %dopar% {
      start_index <- (i - 1) * page_size

      # Get table response
      table_response <- tryCatch(
        {
          table_by_page(start_index)
        },
        error = function(e) {
          futile.logger::flog.error(e$message)
        }
      )

      # Extract the content of the response and convert it to a tibble
      tryCatch(
        {
          table_response %>%
            jsonlite::fromJSON() %>%
            .[["results"]] %>%
            as_tibble()
        },
        error = function(e) {
          msg = paste0("Error in converting response to tibble: ",
                       e$message)
          futile.logger::flog.error(msg)
        }
      )
    }
  }, error = function(e){
    futile.logger::flog.error(paste0("Error in get_data(): ", e))
  })
}
