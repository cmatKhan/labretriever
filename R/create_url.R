#' Create a URL for the given tablename with optional filtering
#'
#' This function creates a URL for the specified tablename by combining
#'   the base URL and the endpoint for the tablename. If provided, it also
#'   appends filter strings to the URL. If a filter list is provided,
#'   a user authentication token is required.
#'
#' @param tablename The name of the table for which to create the URL.
#' @param base_url The base URL of the API
#'   (default: database_info$base_url).
#' @param filter_list A list of filter strings to append to the
#'   URL (default: NULL).
#' @param token The user authentication token required for adding filter
#'   strings (default: NULL).
#'
#' @return The complete URL for the specified tablename, with filter
#'   strings appended if provided.
#'
#' @seealso \code{\link{database_info}}, \code{\link{get_user_auth_token}}
#'
#' @importFrom futile.logger flog.error
#'
#' @examples
#' \dontrun{
#' # Without filters
#' create_url("ccexperiment")
#'
#' # With filters
#' create_url("ccexperiment",
#'             filter_list = list("id=1", "category=sample"),
#'             token = "your_auth_token")
#' }
#' @export
create_url <- function(tablename,
                       base_url = database_info$base_url,
                       filter_list = NULL,
                       token = NULL) {

  # Check if tablename exists in the database_info object
  if (!tablename %in% names(database_info$endpoints)) {
    futile.logger::flog.error(paste('The following tablename does',
                                    'not exist in databaseinfo$endpoints:',
                                    tablename,
                                    sep=" "))
    return(NULL)
  }

  # Combine the base URL and the endpoint for the tablename
  url <- file.path(gsub("/$", "", base_url),
                   gsub("^/", "", database_info$endpoints[[tablename]]))

  # If filter_list is provided and not NULL
  if (!is.null(filter_list)) {
    # If token is not provided or is NULL
    if (is.null(token)) {
      futile.logger::flog.error(paste('To add a filter string,',
                                      'you must provide your user auth',
                                      'token. URL with no filter string',
                                      'has been returned',
                                      sep=" "))
    } else {
      # Apply filters to the URL using the apply_filters_to_url function
      url <- apply_filters_to_url(url, filter_list, token)
    }
  }

  return(url)
}

#' Append an additional endpoint to a given URL
#'
#' This function appends an additional endpoint to a given URL while ensuring
#' that there are no trailing slashes in the base URL.
#'
#' @param url A character string representing the base URL to which the additional
#'   endpoint will be appended.
#' @param additional_endpoint A character string representing the additional
#'   endpoint to be appended to the base URL.
#'
#' @return A character string representing the new URL with the additional
#'   endpoint appended.
#'
#' @export
apply_additional_endpoints = function(url,additional_endpoint){
  file.path(gsub("/$", "", url), additional_endpoint)
}


#' Apply filters to a URL
#'
#' This function appends filter strings to a given URL based on the provided
#' filter_list. It first checks if the filter_list is valid, then appends the
#' filters to the URL.
#'
#' @param url The base URL to which filters should be applied.
#' @param filter_list A named list of filters to apply to the URL.
#' @param token The user authentication token.
#'
#' @return The URL with filters appended if the filter_list is valid, otherwise
#'   returns the original URL.
#'
#' @seealso \code{\link{create_url}}, \code{\link{get_field_info}}
#'
#' @importFrom futile.logger flog.error
#'
#' @examples
#' \dontrun{url <- "https://api.example.com/data"
#' filter_list <- list(id = 1, category = "sample")
#' token <- "your_auth_token"
#' apply_filters_to_url(url, filter_list, token)}
#'
#' @examples
#' # Example usage of apply_additional_endpoints function
#' base_url <- "https://api.example.com/resource/"
#' endpoint <- "count"
#' new_url <- apply_additional_endpoints(base_url, endpoint)
#' print(new_url) # Output: "https://api.example.com/resource/count"
#'
#' @export
apply_filters_to_url <- function(url, filter_list, token) {
  field_info <- labretriever::get_field_info(url, token)

  if (is.null(names(filter_list))) {
    futile.logger::flog.error(paste('filter_list must be a named list.',
                                    'URL returned unchanged', sep=" "))
    return(url)
  } else if (!all(names(filter_list) %in% field_info$filter)) {
    futile.logger::flog.error(paste('the following columns are not',
                                    'filter-able fields in',
                                    url, ': ',
                                    paste(names(filter_list)[
                                      names(filter_list) %in%
                                        field_info$filter],
                                      collapse = ", "),
                                    '. URL returned unchanged',
                                    sep=" "))
    return(url)
  } else {
    filters <- paste(paste(names(filter_list),
                           filter_list, sep = "="),
                     collapse = '&')
    url_list = list(
      url = paste0(url, ifelse(grepl("\\?", url), "&", "?"), filters),
      pagination = paste0(apply_additional_endpoints(
        url,database_info$endpoints$pagination_info),
        ifelse(grepl("\\?", url), "&", "?"), filters),
      count = paste0(apply_additional_endpoints(
        url,database_info$endpoints$row_count),
        ifelse(grepl("\\?", url), "&", "?"), filters),
      fields = paste0(apply_additional_endpoints(
        url,'fields/'),
        ifelse(grepl("\\?", url), "&", "?"), filters)
    )
  }

  return(url_list)
}

