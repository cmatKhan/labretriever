test_that("send() correctly handles field differences and logs messages", {
  # Set up mock server
  mock <- webmockr::enable()
  url <- "http://example.com/api/"
  endpoint <- "myendpoint"
  auth_token <- "my_token"

  # Mock the fields endpoint
  fields_json <- '{
    "readable": ["id", "name", "description"],
    "writable": ["name", "description"],
    "automatically_generated": ["id", "uploader", "uploadDate", "modified"]
  }'

  fields_url <- paste0(url, "myendpoint/", "fields/")
  webmockr::stub_request("get", fields_url) %>%
    webmockr::to_return(
      body = fields_json,
      headers = list("Content-Type" = "application/json"),
      status = 200
    )

  # Create a dataframe for testing
  df <- data.frame(id = 1:10,
                   name = letters[1:10],
                   description = LETTERS[1:10],
                   stringsAsFactors = FALSE)

  # Mock the server endpoint
  response_json <- '{"status": "success"}'
  webmockr::stub_request("post", paste0(url, endpoint)) %>%
    webmockr::to_return(
      body = response_json,
      headers = list("Content-Type" = "application/json"),
      status = 200
    )

  # Capture the log messages
  futile.logger::flog.threshold(futile.logger::INFO)
  log_messages <- testthat::capture_output_lines(
    send(df, paste0(url, endpoint), auth_token)
  )

  # Check if the log messages are as expected
  expected_warning <- paste0(
    "WARN \\[\\d+-\\d+-\\d+ \\d+:\\d+:\\d+\\] ",
    "removing the following columns because they are automatically generated: id"
  )
  testthat::expect_match(log_messages[1], expected_warning)

  expected_info <- paste0(
    "INFO \\[\\d+-\\d+-\\d+ \\d+:\\d+:\\d+\\] ",
    "send\\(\\) HTTP response: category - Success: reason - OK: message - Success: \\(200\\) OK"
  )

  testthat::expect_match(log_messages[2], expected_info)

  # Remove the mock server
  webmockr::disable()
})

test_that("send() correctly updates and logs", {
  # Set up mock server
  mock <- webmockr::enable()
  url <- "http://example.com/api/"
  endpoint <- "myendpoint"
  auth_token <- "my_token"

  # Mock the fields endpoint
  fields_json <- '{
    "readable": ["id", "name", "description"],
    "writable": ["name", "description"],
    "automatically_generated": ["id", "uploader", "uploadDate", "modified"]
  }'

  fields_url <- paste0(url, "myendpoint/", "fields/")
  webmockr::stub_request("get", fields_url) %>%
    webmockr::to_return(
      body = fields_json,
      headers = list("Content-Type" = "application/json"),
      status = 200
    )

  # Create a dataframe for testing
  df <- data.frame(id = 1:10,
                   name = letters[1:10],
                   description = LETTERS[1:10],
                   stringsAsFactors = FALSE)

  # Mock the server endpoint
  response_json <- '{"status": "success"}'
  webmockr::stub_request("put", paste0(url, endpoint, "/1")) %>%
    webmockr::to_return(
      body = response_json,
      headers = list("Content-Type" = "application/json"),
      status = 200
    )
  # Capture the log messages
  futile.logger::flog.threshold(futile.logger::INFO)
  log_messages <- testthat::capture_output_lines(
    send(df[1,], paste0(url, endpoint), auth_token, update = TRUE)
  )

  # Check if the log messages are as expected
  expected_info <- paste0(
    "INFO \\[\\d+-\\d+-\\d+ \\d+:\\d+:\\d+\\] ",
    "send\\(\\) HTTP response: category - Success: reason - OK: message - Success: \\(200\\) OK"
  )

  testthat::expect_match(log_messages[1], expected_info)

  # Remove the mock server
  webmockr::disable()
})
