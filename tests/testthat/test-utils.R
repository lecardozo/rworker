context('Testing utility functions')

test_that('ncores function works', {
    expect_error(ncores(), NA)
})
