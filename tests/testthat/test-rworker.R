context('Rworker methods')

TestRworker = R6::R6Class("TestRworker",
                        inherit = Rworker,
                        lock_objects=F,
                        active = list(
                            rscript = function() {return(private$rscript)},
                            wproc = function() {return(private$wproc)}
                        ))
name = 'celery'
queue = 'redis://localhost:6379'
backend = queue
workers = 2

dummy = function() { print('It worked') }

rwork = TestRworker$new(name=name, queue=queue, backend=backend,
                        workers=workers)

test_that('Rworker initilization works just fine', {
    expect_equal(rwork$queue_url, queue)
    expect_equal(rwork$qname, name)
    expect_equal(rwork$workers, workers)
    expect_equal(rwork$backend_url, backend)
    expect_true(rwork$rscript != '')
    expect_true(rwork$wproc != '')
})

test_that('Rworker$start_pool spawns correct number of processes', {
    rwork$start_pool(workers)
    expect_equal(length(rwork$pool), workers)
})

test_that('Rworker$pool is really running', {
    running = sum(unlist(lapply(rwork$pool, function(p) p$is_alive())))
    expect_equal(running, workers)
})

test_that('Rworker$start_pool is idempotent', {
    expect_warning(rwork$start_pool(workers), 'already running')
})

test_that('Rworker$kill_pool kills processes', {
    rwork$kill_pool()
    expect_equal(length(rwork$pool), 0)
})

test_that('Tasks are successfully registred', {
    rwork$task(dummy, name='dummy')
    expect_named(rwork$tasks, 'dummy')
    expect_equal(rwork$tasks[['dummy']], dummy)
})

context('Rworker and worker communication')

test_that('Rworker~worker communication works', {
    rwork$bootstrap_cluster(workers, pipe=T)

    tereq <- list()
    tereq$task <- dummy
    tereq$kwargs <- list()
    tereq$task_id <- 123
    tereq$warnings <- list()
    tereq$errors <- list()
    tereq$status <- 'PENDING'

    rwork$execute(tereq)
    
    # give some time for communication
    Sys.sleep(0.2)

    print(rwork$pool)
    outputs = lapply(rwork$pool, function(p) {p$read_output_lines()})
    errors = lapply(rwork$pool, function(p) {p$read_error_lines()})
    
    
    expect_equal(sum(grepl('worked', outputs)), 1)
    
    lapply(errors, function(e) {
        expect_equal(e, character(0))                    
    })
})

test_that('Message load balancing happens', {
    
    for (i in 1:2) {
        rwork$execute(list(task=dummy, kwargs=list(), task_id=123, warnings=list(), errors=list()))
    }
    
    # give some time for communication
    Sys.sleep(0.2)

    outputs = lapply(rwork$pool, function(p) {p$read_output_lines()})
    errors = lapply(rwork$pool, function(p) {p$read_error_lines()})
    
    expect_equal(sum(grepl('worked', outputs)), workers)
    
    lapply(errors, function(e) {
        expect_equal(e, character(0))                    
    })
})
