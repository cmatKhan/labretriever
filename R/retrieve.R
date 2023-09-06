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
  # if successful, this will be overwritten
  output <- NULL
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
    output <- httr::content(response)$token
  } else {
    futile.logger::flog.error(
      labretriever::extend_msg_error(response_msg, response, url)
    )
  }

  output
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
  # if successful, this will be overwritten
  output <- NULL
  # TODO error handling -- should the slots all have entries, eg?

  # ensure that fields_endpoint ends in a /
  fields_endpoint <- paste0(gsub("/+$", "", fields_endpoint), "/")

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
    output <- httr::content(response)
  } else {
    futile.logger::flog.error(labretriever::extend_msg_error(
      response_msg,
      response,
      fields_url
    ))
  }

  output
}

#' Retrieve Paginated Data From a REST API
#'
#' This function retrieves all data from a paginated REST API. It will continue
#' to request data until all pages have been retrieved.
#'
#' @param init_url The initial URL from which to start retrieving data.
#' @param token The authorization token for the API.
#'
#' @return A tibble containing all data retrieved from the API.
#'
#' @examples
#' \dontrun{
#'   get_paginated_data("https://api.example.com/data",
#'                          "your_token")
#' }
#'
#' @importFrom httr GET add_headers content
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr as_tibble bind_rows
#' @importFrom futile.logger flog.debug
#' @export
get_paginated_data <- function(init_url, token) {

  data_list = list()

  header <- httr::add_headers(
    "Authorization" = paste("token", token, sep = " "),
    "Content-Type" = "application/json"
  )

  current_url <- init_url
  page_index = 1
  futile.logger::flog.info(paste0('retrieving data from: ', init_url))
  while (!is.null(current_url)) {
    futile.logger::flog.debug(paste0('pulling page: ',
                                     as.character(page_index)))
    response <- httr::GET(current_url, header)

    parsed <- jsonlite::fromJSON(
      httr::content(response, "text", encoding='utf-8'),
      flatten = TRUE)

    data_list[[length(data_list) + 1]] <- parsed$results

    current_url <- parsed$`next`
    page_index = page_index + 1
  }

  dplyr::as_tibble(dplyr::bind_rows(data_list))
}

#' Retrieve from a REST API with pagination
#'
#' This function retrieves data from a REST API that uses pagination.
#'
#' @param url The base URL of the endpoint, such as
#'   "http://127.0.0.1:8000/api/v1/genes/".
#' @param token Your API authentication token, if required.
#' @param retrieve_all Boolean. Default TRUE. Set to FALSE to return
#'   only a single page, if the data is paginated
#' @inheritDotParams apply_filters_to_url filter_list
#'
#' @return A tibble containing the data retrieved from the API
#'
#' @importFrom httr GET content status_code http_status
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr as_tibble
#' @importFrom futile.logger flog.info flog.error
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
retrieve <- function(url, token, retrieve_all = TRUE, ...) {
  # If successful, this will be overwritten
  output <- NULL
  # extract additional arguments
  parameters <- list(...)

  if ("filter_list" %in% names(parameters)) {
    url_list <- labretriever::apply_filters_to_url(
      url,
      parameters$filter_list,
      token
    )
    url <- url_list$url
  }

  tryCatch(
    {
      if (retrieve_all) {
        output <- get_paginated_data(url, token)
      } else{
        # Retrieve specified page
        futile.logger::flog.info(paste0('retrieving data from: ', url))
        response <- httr::GET(
          url,
          httr::add_headers(Authorization = token))
        if (httr::status_code(response) == 200) {
          output <-  jsonlite::fromJSON(
            httr::content(response, "text", encoding='utf-8'),
            flatten = TRUE) %>%
            .[['results']] %>%
            as_tibble()
        } else {
          futile.logger::flog.error(paste0(
            "Failed to retrieve data from ",
            url,
            ". HTTP Status: ",
            httr::http_status(response)$message
          ))
        }
      }
    },
    error = function(e) {
      futile.logger::flog.error(paste0("Error: ", e$message))
    })
  # return output -- it may be NULL
  output
}

