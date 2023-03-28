#' Get User Authentication Token
#'
#' This function retrieves an authentication token from a specified URL
#'   using a username and password.
#'
#' @param url A character string specifying the view against which to send
#'   a request. This should be constructed using database_info -- see the
#'   example.
#' @param username A character string specifying the user's username.
#' @param password A character string specifying the user's password.
#'
#' @return The authentication token as a character string.
#'
#' @seealso \code{\link{database_info}}
#'
#' @importFrom httr http_status content POST
#' @importFrom futile.logger flog.debug flog.error
#'
#' @note do not save your auth token in a public repository.
#'   For example, you might put it in your .Renviron and then make sure
#'   that your .Renviron is in your .gitignore. Otherwise, save it outside
#'   of a github tracked directory or otherwise ensure that it
#'   will not be pushed up to github
#'
#' @examples
#' \dontrun{
#' # Note: This example is wrapped in \dontrun{} because it requires a valid API
#' # endpoint and authentication credentials. Please replace the example URL,
#' # username, and password with your actual API endpoint and credentials to test
#' # the function.
#'
#' # Get the user authentication token
#' auth_url <- "https://your-api.com/api/auth-token/"
#' username <- "your_username"
#' password <- "your_password"
#' token <- get_user_auth_token(auth_url, username, password)
#' print(token)
#'
#' # Set the token as an environment variable
#' # usethis::edit_r_environ("project")
#' # This opens a file called .Renviron in the project directory
#'
#' # IMPORTANT: Immediately create a .gitignore and add
#' # .Renviron to it to avoid committing the login token to Github
#'
#' # Add your token to the .Renviron file like this
#' # TOKEN=<your token>
#'
#' # Reload the project, and now you can access the environmental
#' # variable token
#' # print(Sys.getenv("TOKEN"))
#'
#' # Define the count endpoint URL
#' record_count_url <- "https://your-api.com/data_table/count/"
#'
#' # Use the authentication token with httr like this:
#' # token <- Sys.getenv("TOKEN")
#' my_api_call <- labretreiver::retrieve(record_count_url, token)
#'
#' # Get the content of the API call
#' httr::content(my_api_call)
#' }
#'
#' @export
get_user_auth_token <- function(url, username, password) {
  # see package httr for help
  response <- httr::POST(
    url = url,
    body = list(
      username = username,
      password = password
    ),
    encode = "json"
  )

  response_msg <- labretriever::construct_basic_response_msg(
    as.character(match.call()[[1]]),
    response
  )

  if (httr::http_status(response)$category == "Success") {
    message(paste("You might want to put your token in your .Renviron.",
      "If you do, please make sure the .Renviron file",
      "is in your .gitignore",
      sep = " "
    ))
    futile.logger::flog.debug(response_msg)
    httr::content(response)$token
  } else {
    futile.logger::flog.error(
      labretriever::extend_msg_error(response_msg, response, url)
    )
  }
}


#' Get pagination information from a REST_FRAMEWORK endpoint
#'
#' This function retrieves the pagination information for a given
#' REST_FRAMEWORK endpoint. It returns a list with the default page
#' size and page size limit.
#'
#' @inheritParams get_user_auth_token
#'
#' @param token The authentication token to use when accessing the endpoint
#' @param pagination_endpoint By default, this is set to
#'   database_info$endpoints$pagination_info and will be appended appropriately
#'   to the url. For example, if you are interested in retrieving the
#'   pagination settings for the following table:
#'   http://127.0.0.1:8000/api/v1/genes/, then you would pass this url, with
#'   or without the trailing /, and the pagination_endpoint, if it is not
#'   provided in the database_info. The URL which will be queried will be
#'   something along the lines of:
#'   http://127.0.0.1:8000/api/v1/genes/pagination_info/. Of course, this
#'   endpoint must exist for this to work.
#'
#' @return A list with the default page size and page size limit
#'
#' @seealso \code{\link{get_user_auth_token}},
#'   \code{\link{database_info}}
#'
#' @examples
#' \dontrun{
#' token <- get_user_auth_token(
#'   paste0(
#'     database_info$base_url,
#'     database_info$endpoints$auth_token
#'   ),
#'   "username",
#'   "password"
#' )
#'
#' url <- paste0(database_info$base_url, database_info$endpoints$chrmap)
#' # Retrieve the pagination information
#' pagination_info <- get_pagination_info(url, token)
#'
#' print(pagination_info)
#' }
#'
#' @importFrom httr GET add_headers http_status status_code content
#' @importFrom futile.logger flog.debug flog.error
#'
#' @export
get_pagination_info <- function(
    url,
    token,
    pagination_endpoint = database_info$endpoints$pagination_info) {
  # construct pagination end point
  pagination_url <- file.path(gsub("/$", "", url), pagination_endpoint)

  # Retrieve the page size limit from the REST_FRAMEWORK settings
  response <- httr::GET(
    pagination_url,
    httr::add_headers(
      "Authorization" = paste("token", token, sep = " "),
      "Content-Type" = "application/json"
    )
  )

  response_msg <- labretriever::construct_basic_response_msg(
    as.character(match.call()[[1]]),
    response
  )

  if (httr::http_status(response)$category == "Success") {
    futile.logger::flog.debug(response_msg)
    # return a list, eg list(default_page_size = 10, page_size_limit = 10)
    httr::content(response)
  } else {
    futile.logger::flog.error(
      labretriever::extend_msg_error(
        response_msg,
        response,
        pagination_url
      )
    )
  }
}


#' Retrieve the fields of a given table from a fields endpoint
#'
#' This function retrieves the fields of a given table from a fields
#'   endpoint and returns them as a list containing readable fields,
#'   writable fields, and automatically generated fields.
#'
#' @param url The base URL of the endpoint, such as
#'   "http://127.0.0.1:8000/api/v1/genes/".
#' @param token Your API authentication token, if required.
#' @param fields_endpoint The endpoint for retrieving the fields,
#'   by default set to "fields/".
#'
#' @return A list containing the readable fields, writable fields, and
#'   automatically generated fields.
#'
#' @examples
#' \dontrun{
#' # Get the fields of a given table from a fields endpoint
#' url <- "https://example.com/api/genes/"
#' token <- "my_token"
#' table_fields <- get_field_info(url, token)
#' }
#'
#' @importFrom httr GET add_headers status_code content
#' @importFrom futile.logger flog.debug flog.error
#'
#' @export
get_field_info <- function(
    url,
    token,
    fields_endpoint = "fields/") {

  # TODO error handling -- should the slots all have entries, eg?

  # ensure that fields_endpoint ends in a /
  fields_endpoint = paste0(gsub("/+$", "", fields_endpoint), "/")

  # construct fields endpoint
  fields_url <- file.path(gsub("/$", "", url), fields_endpoint)

  # retrieve data from the endpoint
  response <- httr::GET(
    fields_url,
    httr::add_headers(
      "Authorization" = paste("token", token, sep = " "),
      "Content-Type" = "application/json"
    )
  )

  response_msg <- labretriever::construct_basic_response_msg(
    as.character(match.call()[[1]]),
    response
  )

  if (httr::http_status(response)$category == "Success") {
    futile.logger::flog.debug(response_msg)
    # return the fields of the table as a list
    httr::content(response)
  } else {
    futile.logger::flog.error(labretriever::extend_msg_error(
      response_msg,
      response,
      fields_url
    ))
  }
}


#' Retrieve the total number of records from a count endpoint
#'
#' This function retrieves the total number of records from a count
#'   endpoint and returns it as a numeric value.
#'
#' @inheritParams get_user_auth_token
#' @inheritParams get_pagination_info
#'
#' @param count_endpoint By default, this is set to
#'   database_info$endpoints$row_count and will be appended appropriately
#'   to the url. For example, if you are interested in retrieving the
#'   row count for the following table:
#'   http://127.0.0.1:8000/api/v1/genes/, then you would pass this url, with
#'   or without the trailing /, and the count_endpoint, if it is not
#'   provided in the database_info. The URL which will be queried will be
#'   something along the lines of:
#'   http://127.0.0.1:8000/api/v1/genes/count/. Of course, this
#'   endpoint must exist for this to work.
#'
#' @return The total number of records as a numeric value.
#'
#' @examples
#' \dontrun{
#' # Get the total number of records from a count endpoint
#' count_url <- "https://example.com/api/count"
#' token <- "my_token"
#' total_records <- get_record_count(count_url, token)
#' }
#'
#' @seealso \code{\link{database_info}},
#'   \code{\link{get_user_auth_token}}
#'
#' @inheritParams get_pagination_info
#' @inheritParams get_user_auth_token
#'
#' @importFrom httr GET add_headers status_code content
#' @importFrom futile.logger flog.debug flog.error
#'
#' @export
get_record_count <- function(
    url,
    token,
    count_endpoint = database_info$endpoints$row_count) {
  # construct row count endpoint
  count_url = ifelse(count_endpoint=='',
                     url,
                     file.path(gsub("/$", "", url), count_endpoint))

  # retrieve data from the endpoint
  response <- httr::GET(
    count_url,
    httr::add_headers(
      "Authorization" = paste("token", token, sep = " "),
      "Content-Type" = "application/json"
    )
  )

  response_msg <- labretriever::construct_basic_response_msg(
    as.character(match.call()[[1]]),
    response
  )

  if (httr::http_status(response)$category == "Success") {
    futile.logger::flog.debug(response_msg)
    # return a of the table fields
    httr::content(response)
  } else {
    futile.logger::flog.error(labretriever::extend_msg_error(
      response_msg,
      response,
      count_url
    ))
  }
}


#' A factory function to return a function to get paginated results by chunk
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

    index_string = paste0("limit=",page_size,"&offset=",start_index)
    curr_url= paste0(url, ifelse(grepl("\\?", url), "&", "?"), index_string)

    response <- httr::GET(
      curr_url,
      httr::add_headers(
        "Authorization" = paste("token", token, sep = " "),
        "Content-Type" = "application/json"
      )
    )

    response_msg <- labretriever::construct_basic_response_msg(
      as.character(match.call()[[1]]),
      response
    )

    if (httr::http_status(response)$category == "Success") {
      futile.logger::flog.debug(response_msg)
      # return the content of the response
      httr::content(response, "text", encoding = "UTF-8")
    } else {
      futile.logger::flog.error(
        labretriever::extend_msg_error(
          response_msg,
          response,
          curr_url
        )
      )
    }
  }
  # return the inner function to the caller
  inner
}


#' Retrieve from a REST API with pagination
#'
#' This function retrieves data from a REST API that uses pagination.
#'
#' @inheritParams get_pagination_info
#' @inheritDotParams apply_filters_to_url filter_list
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
#' retrieve("https://example.com/api", "1234")
#' }
#'
#' @seealso \code{\link{get_pagination_info}}, \code{\link{get_record_count}}
#'
#' @export
retrieve <- function(
    url,
    token, ...) {
  if ('filter_list' %in% names(list(...))){
      url_list = labretriever::apply_filters_to_url(url,list(...)$filter_list,token)
      url = url_list$url
      pagination_info = get_pagination_info(url_list$pagination, token, '')
      total_records = get_record_count(url_list$count, token, '')
  } else{

    # Get pagination URL and info
    pagination_info <- get_pagination_info(url, token)

    # Get total number of records
    total_records <- get_record_count(url, token)
  }

  tryCatch(
    {
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
            # TODO change this to a progress bar
            page_log_msg = paste('retrieving up to', as.character(page_size),
                                 'records from page',as.character(i),
                                 'of', as.character(total_pages),
                                 sep=' ')
            futile.logger::flog.info(page_log_msg)
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
            msg <- paste0(
              "Error in converting response to tibble: ",
              e$message
            )
            futile.logger::flog.error(msg)
          }
        )
      }
    },
    error = function(e) {
      futile.logger::flog.error(
        paste0("Error in ", match.call()[[1]], "(): ", e)
      )
    }
  )
}
