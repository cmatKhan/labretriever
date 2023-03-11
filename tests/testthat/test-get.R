test_that("get_pagination_info() returns json describing pagination settings", {
  # Set up mock server
  mock <- webmockr::enable()
  url <- "http://example.com/api/"
  pagination_url <- paste0(url, database_info$endpoints$pagination_info)
  token <- "my_token"

  # Mock the pagination endpoint
  pagination_json <- '{"page_size_limit":10, "default_page_size":10}'
  webmockr::stub_request("get",pagination_url) %>%
    webmockr::to_return(
      body = pagination_json,
      headers = list("Content-Type" = "application/json"),
      status = 200)

  # Call the function and check the results
  #results <- get_data(url, token, pagination_endpoint = "pagination/", count_endpoint = "count/")
  res = labretriever::get_pagination_info(pagination_url, token)

  testthat::expect_equal(res$page_size_limit, 10)
  testthat::expect_equal(res$default_page_size, 10)

  # Remove the mock server
  webmockr::disable()
})


test_that("test get_total_records() returns json with field count", {
  # Set up mock server
  mock <- webmockr::enable()
  url <- "http://example.com/api/"
  record_count_url <- paste0(url, database_info$endpoints$row_count)
  token <- "my_token"

  # Mock the count endpoint
  count_json <- '{"count": 1000}'
  webmockr::stub_request("get",record_count_url) %>%
    webmockr::to_return(
      body = count_json,
      headers = list("Content-Type" = "application/json"),
      status = 200)

  # Call the function and check the results
  #results <- get_data(url, token, pagination_endpoint = "pagination/", count_endpoint = "count/")
  res = labretriever::get_total_records(record_count_url, token)
  testthat::expect_equal(res$count, 1000)

  # Remove the mock server
  webmockr::disable()
})


test_that("get_table_by_page() logs HTTP responses", {
  mock <- webmockr::enable()
  url <- "http://example.com/api/"
  pagination_url <- paste0(url, database_info$endpoints$pagination_info)
  token <- "my_token"

  # Mock the pagination endpoint
  pagination_json <- '{"page_size_limit":10, "default_page_size":10}'
  webmockr::stub_request("get",pagination_url) %>%
    webmockr::to_return(
      body = pagination_json,
      headers = list("Content-Type" = "application/json"),
      status = 200)


  record_count_url <- paste0(url, database_info$endpoints$row_count)

  # Mock the count endpoint
  count_json <- '{"count": 1000}'
  webmockr::stub_request("get",record_count_url) %>%
    webmockr::to_return(
      body = count_json,
      headers = list("Content-Type" = "application/json"),
      status = 200)

  # Define mock responses for the data URL
  data_url = paste0(url,"data/")
  paginated_data_url <- paste0(data_url,'?pageSize=10&startIndex=0')
  data_json <- '{"results": [{"col1": "value1", "col2": "value2"}]}'
  webmockr::stub_request("GET", paginated_data_url) %>%
    webmockr::to_return(
      body = data_json,
      headers = list(`Content-Type` = "application/json"),
      status = 200)

  data_url

  # Call the function with a mock URL and a page size of 10
  token <- "my_token"
  get_table <- labretriever::get_table_by_page(
    data_url,
    token,
    page_size = 10)

  futile.logger::flog.threshold(futile.logger::DEBUG)
  captured_output <- testthat::capture_output(get_table(0))
  futile.logger::flog.threshold(futile.logger::INFO)

  # Check that the log messages were generated
  actual = gsub("DEBUG \\[\\d+-\\d+-\\d+ \\d+:\\d+:\\d+\\] ",
                "",captured_output[1])
  expected = paste0("get_table() HTTP response: category - Success: reason ",
                    "- OK: message - Success: (200) OK")

  testthat::expect_equal(actual, expected)

  webmockr::disable()
})

test_that("get_data() retrieves data correctly", {
  mock = webmockr::enable()
  # Define URLs and pagination/count info
  url <- "http://mock-api.com"
  pagination_endpoint <- database_info$endpoints$pagination_info
  count_endpoint <- database_info$endpoints$row_count
  pagination_url <- file.path(gsub("/$", "", url), pagination_endpoint)
  pagination_json <- '{"default_page_size": 10, "page_size_limit": 100}'
  count_url <- file.path(gsub("/$", "", url), count_endpoint)
  count_json <- '{"count": 10}'

  # Set up mock requests
  webmockr::stub_request("get", pagination_url) %>%
    webmockr::to_return(
      body = pagination_json,
      headers = list("Content-Type" = "application/json"),
      status = 200
    )

  webmockr::stub_request("get", count_url) %>%
    webmockr::to_return(
      body = count_json,
      headers = list("Content-Type" = "application/json"),
      status = 200
    )

  # Define expected results
  page_size <- jsonlite::fromJSON(pagination_json)$page_size_limit
  total_pages <- ceiling(jsonlite::fromJSON(count_json)$count / page_size)


  # Define mock responses for the data URL
  data_url = paste0(url)
  paginated_data_url <- paste0(data_url,'?pageSize=100&startIndex=0')
  data_json <- '{"results": [{"col1": "value1", "col2": "value2"}]}'
  webmockr::stub_request("GET", paginated_data_url) %>%
    webmockr::to_return(
      body = data_json,
      headers = list(`Content-Type` = "application/json"),
      status = 200)

  # Set up expectations for logging
  expect_silent(futile.logger::flog.threshold("ERROR"))

  # Call get_data()
  results <- labretriever::get_data(url, "dummy_token")

  # Check results
  testthat::expect_equal(nrow(results), total_pages)
  testthat::expect_equal(class(results)[[1]], "tbl_df")

  # Clean up mock requests
  webmockr::disable()

})
