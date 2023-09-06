test_that("get_user_auth_token returns auth token for valid credentials", {
  # Set up mock server
  mock <- webmockr::enable()
  on.exit(webmockr::disable())
  url <- "http://example.com/auth/"
  token_url <- paste0(url, "token/")
  username <- "test_user"
  password <- "test_password"
  expected_token <- "my_token"

  # Mock the token endpoint
  token_json <- paste0('{"token":"', expected_token, '"}')
  webmockr::stub_request("post", token_url) %>%
    webmockr::to_return(
      body = token_json,
      headers = list("Content-Type" = "application/json"),
      status = 200
    )

  # Call the function and check the results
  res <- get_user_auth_token(token_url, username, password)
  testthat::expect_equal(res, expected_token)
})

test_that("get_user_auth_token returns error message for invalid credentials", {
  # Set up mock server
  mock <- webmockr::enable()
  on.exit(webmockr::disable())

  url <- "http://example.com/auth/"
  token_url <- paste0(url, "token/")
  username <- "test_user"
  password <- "invalid_password"

  # Mock the token endpoint
  error_msg <- "Invalid username or password"
  webmockr::stub_request("post", token_url) %>%
    webmockr::to_return(
      body = error_msg,
      headers = list("Content-Type" = "text/plain"),
      status = 401
    )

  res <- capture.output(get_user_auth_token(token_url, username, password))

  # parse out message for testing
  actual_1 <- gsub(
    "ERROR \\[\\d+-\\d+-\\d+ \\d+:\\d+:\\d+\\] ",
    "", res[1]
  )
  actual_2 = res[2]
  actual_3 = res[3]

  expected_1 <- paste0(
    "get_user_auth_token() HTTP response: category - ",
    "Client error: reason - Unauthorized: message - ",
    "Client error: (401) Unauthorized."
  )
  expected_2 = "\tDetails: Invalid username or password"
  expected_3 = "\tURL: http://example.com/auth/token/"

  testthat::expect_equal(actual_1, expected_1)
  testthat::expect_equal(actual_2, expected_2)
  testthat::expect_equal(actual_3, expected_3)
})


test_that("test get_field_info() returns json with fields", {
  # Set up mock server
  mock <- webmockr::enable()
  on.exit(webmockr::disable())
  url <- "http://example.com/api/genes/"
  readable_fields_url <- paste0(url, "fields/")
  token <- "my_token"

  # Mock the readable_fields endpoint
  fields_json <- '{
    "readable": ["id", "name", "description"],
    "writable": ["name", "description"],
    "automatically_generated": ["id", "uploader", "uploadDate", "modified"],
    "filter": ["name", "description"]
  }'
  webmockr::stub_request("get", readable_fields_url) %>%
    webmockr::to_return(
      body = fields_json,
      headers = list("Content-Type" = "application/json"),
      status = 200
    )
  # Call the function and check the results
  res <- get_field_info(url, token)
  testthat::expect_equal(names(res),
                         c("readable", "writable",
                           "automatically_generated", "filter"))
})

test_that("get_paginated_data() retrieves data correctly", {
  mock <- webmockr::enable()
  on.exit(webmockr::disable())

  # Define URLs and pagination/count info
  url <- "http://mock-api.com"
  token <- "test_token"
  total_pages <- 5

  # Mock responses for each page
  for (i in 1:total_pages) {
    response_body = list()
    response_body[['count']] = total_pages
    response_body[['next']] = if (i < total_pages){
      paste0(url, "?page=", i + 1)
      } else{
        NULL
      }
    response_body[['results']] = data.frame(id = seq((i - 1) * 10 + 1, i * 10))
    stub <- webmockr::stub_request(
      "get", paste0(url,
                    if (i > 1) paste0("?page=", i) else ""))
    webmockr::to_return(stub,
                        status = 200,
                        body = response_body,
                        headers = list("Content-Type" = "application/json"))
  }

  # Get data
  results <- get_paginated_data(url, token)

  # Check results
  testthat::expect_equal(nrow(results), total_pages * 10)
  testthat::expect_equal(class(results)[[1]], "tbl_df")
})

test_that("retrieve() retrieves data correctly", {
  mock <- webmockr::enable()
  on.exit(webmockr::disable())

  # Define URLs and pagination/count info
  url <- "http://mock-api.com"
  token <- "test_token"
  total_pages <- 5

  # Mock responses for each page
  for (i in 1:total_pages) {
    response_body = list()
    response_body[['count']] = total_pages
    response_body[['next']] = if (i < total_pages){
      paste0(url, "?page=", i + 1)
      } else{
        NULL
      }
    response_body[['results']] = data.frame(id = seq((i - 1) * 10 + 1, i * 10))
    stub <- webmockr::stub_request(
      "get",
      paste0(url, if (i > 1) paste0("?page=", i) else ""))
    webmockr::to_return(stub,
                        status = 200,
                        body = response_body,
                        headers = list("Content-Type" = "application/json"))
  }

  # Test retrieval of all data
  results <- retrieve(url, token)
  testthat::expect_equal(nrow(results), total_pages * 10)
  testthat::expect_equal(class(results)[[1]], "tbl_df")

  # Test retrieval of a single page
  results_page_1 <- retrieve(url, token, retrieve_all = FALSE)
  testthat::expect_equal(nrow(results_page_1), 10)
  testthat::expect_equal(class(results_page_1)[[1]], "tbl_df")

})

