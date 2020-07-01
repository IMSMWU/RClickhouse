context("config")



test_that("correct computation of difference of parameter sets", {
  config <- c(host='localhost', port=9000, db='non-default-db')
  config_temp <- c(host='localhost', port=9000, db='default', user='default', password='', compression='lz4')
  diff_params <- checkParameters(config, config_temp)
  expect_equal(diff_params, names(c(user='default', password='', compression='lz4')))
})


test_that("correct completion of existing input/config", {
  config <- c(host='localhost', port=9000, db='non-default-db')
  config_temp <- c(host='localhost', port=9000, db='default', user='default', password='')
  config_compl <- complementList(config, config_temp)
  expect_equal(config_compl, c(host='localhost', port=9000, db='non-default-db', user='default', password=''))
})


test_that("correct loading of and merging of different parameter sources", {
  CONFIG_PATHS <- paste(getwd(), '/fixture_config.yaml', sep='')
  DEFAULT_PARAMS <- c(host='localhost', port=9000, db='default', user='default', password='', compression='lz4')
  input_params <- c(host='customhost', db='input-db')
  default_input_diff <- c(input_params[!(input_params %in% DEFAULT_PARAMS)])

  expect_warning({
      config <- loadConfig(CONFIG_PATHS, DEFAULT_PARAMS, default_input_diff)
    })

  expect_equal(config, c(host='customhost', db='input-db', port=6666, user='default', password='', compression='lz4'))
})
