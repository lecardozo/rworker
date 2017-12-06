context('Tasks')

dummy_function <- function() {
    x = 2
    if (x == 2) {
        x = 3
    }
}

tasks <- Tasks$new(tasklist=list(dummy_function=dummy_function))

test_that('Tasks object generate tmp file', {
    expect_true(file.exists(tasks$src))
})

test_that('Tasks object generate correct tmp file', {
    expect_error(source(tasks$src), NA)
})
