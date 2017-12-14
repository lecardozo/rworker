Sys.setenv(R_TESTS="")

library(testthat)
library(rworker)

test_check("rworker")
