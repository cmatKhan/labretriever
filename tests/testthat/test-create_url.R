test_that("create_url returns a valid URL for existing tablename", {
  # Call the function and check the results
  url <- create_url(names(database_info$endpoints)[[1]])

  expected_url <- paste0(database_info$base_url, database_info$endpoints[[1]])
  testthat::expect_equal(url, expected_url)
})

test_that("create_url logs an error message for non-existent tablename", {
  # Set up test data
  tablename <- "nonexistent_table"

  # Call the function and check the results
  res <- capture.output(
    create_url(tablename,
               base_url = "http://example.com/api"))
  actual <- gsub(
    "ERROR \\[\\d+-\\d+-\\d+ \\d+:\\d+:\\d+\\] ",
    "", res[[1]])
  expected <- paste0(
    "The following tablename does not exist in ",
    "databaseinfo\\$endpoints: nonexistent_table"
  )

  testthat::expect_match(actual, expected)
})

test_apply_additional_endpoints <- function() {
  # Test case 1: Appending an additional endpoint without trailing slash
  base_url_1 <- "https://api.example.com/resource"
  endpoint_1 <- "count"
  expected_url_1 <- "https://api.example.com/resource/count"
  test_that("Appending an additional endpoint without trailing slash works", {
    expect_equal(apply_additional_endpoints(base_url_1, endpoint_1), expected_url_1)
  })

  # Test case 2: Appending an additional endpoint with trailing slash
  base_url_2 <- "https://api.example.com/resource/"
  endpoint_2 <- "count"
  expected_url_2 <- "https://api.example.com/resource/count"
  test_that("Appending an additional endpoint with trailing slash works", {
    expect_equal(apply_additional_endpoints(base_url_2, endpoint_2), expected_url_2)
  })

  # Test case 3: Appending an empty additional endpoint
  base_url_3 <- "https://api.example.com/resource/"
  endpoint_3 <- ""
  expected_url_3 <- "https://api.example.com/resource"
  test_that("Appending an empty additional endpoint works", {
    expect_equal(apply_additional_endpoints(base_url_3, endpoint_3), expected_url_3)
  })
}

test_that("test apply_filters_to_url() appends valid filters", {
  # Set up mock server
  mock <- webmockr::enable()
  url <- "http://example.com/api/genes/"
  readable_fields_url <- apply_additional_endpoints(url, "fields/")
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
  # Call the function with valid filters
  filter_list <- list(name = "geneA", description = "test")
  filtered_url_list <- apply_filters_to_url(url, filter_list, token)

  # Check the results
  expected_url <- paste0(url, "?name=geneA&description=test")
  testthat::expect_equal(filtered_url_list$url, expected_url)

  # Check the results for pagination, count, and fields endpoints
  expected_pagination_url <- paste0(apply_additional_endpoints(url, "pagination_info/"), "?name=geneA&description=test")
  expected_count_url <- paste0(apply_additional_endpoints(url, "count/"), "?name=geneA&description=test")
  expected_fields_url <- paste0(apply_additional_endpoints(url, "fields/"), "?name=geneA&description=test")

  testthat::expect_equal(filtered_url_list$pagination, expected_pagination_url)
  testthat::expect_equal(filtered_url_list$count, expected_count_url)
  testthat::expect_equal(filtered_url_list$fields, expected_fields_url)

  # TODO test logging on error conditions

  webmockr::disable()
})
