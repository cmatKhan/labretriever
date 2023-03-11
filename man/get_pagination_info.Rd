% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/get.R
\name{get_pagination_info}
\alias{get_pagination_info}
\title{Get pagination information from a REST_FRAMEWORK endpoint}
\usage{
get_pagination_info(pagination_url, token)
}
\arguments{
\item{pagination_url}{The URL of the REST_FRAMEWORK endpoint for
which to retrieve the pagination information}

\item{token}{The authentication token to use when accessing the endpoint}
}
\value{
A list with the default page size and page size limit
}
\description{
This function retrieves the pagination information for a given REST_FRAMEWORK
endpoint. It returns a list with the default page size and page size limit.
}
\examples{
\dontrun{
pagination_url <- "http://127.0.0.1:8000/api/v1/chrmap/pagination_info/"
token <- get_user_auth_token(
  "http://example.com/api/get_token",
  "user", "password"
)

# Retrieve the pagination information
pagination_info <- get_pagination_info(pagination_url, token)
}

}
\seealso{
\code{\link{get_user_auth_token}}
}