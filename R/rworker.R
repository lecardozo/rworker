#' @importFrom R6 R6Class
#' @import redux
#' @importFrom jsonlite fromJSON toJSON
#' @importFrom base64enc base64decode
#' @importFrom processx process
#' @import glue
#' @import rzmq
NULL

#' Rworker object
#'
#' The Rworker object consumes messages sent to the message
#' broker and sends tasks to be executed in background by the
#' process pool.
#'
#' @section Usage:
#' ```
#' rwork <- rworker()
#' ```
#'
#' 
#' @param name The name of the message queue.
#' @param workers The number of background worker processes.
#' @param queue A url string of type "provider://host:port".
#' @param backend A url string of type "provider://host:port".
#' 
#' @section Details:
#' `$new()` creates new Rworker instance. Process pool is started and Queue 
#'  connection is established during instantiation.
#'
#' `$start_pool()` starts all processes in the background process pool
#'
#' `$kill_pool()` kills all processes in the background process pool
#'
#' `$pool` processes list
#'
#' `$task()` registers function as task to be remotelly executed
#'
#' `$tasks` list of registered tasks
#'
#' `$consume()` listens for message broker messages and send them to be executed
#'  by the worker process pool
#'
#' `$execute()` method used to send tasks and arguments for background execution
#'
#' `$update_state()` method used to gather tasks execution status from worker pool
#'
#' `register_backend()` registers results backend
#'
#' @rdname Rworker
#' @name Rworker
#' @examples
#' \dontrun{
#' rwork <- rworker()
#'
#' # Register task
#' myfun <- function() { 
#'     Sys.sleep(5)
#' }
#' rwork$task(myfun, name='long_running_task')
#'
#' # Send task for background execution
#' rwork$execute('long_running_task')
#'
#' # Listen to messages from message queue
#' rwork$consume()
#'
#' }
#'
NULL

#' @export
Rworker <- R6::R6Class(
    'Rworker',
    lock_objects=FALSE,
    active = list(
        pool = function() {
            return(private$workers_list)
        },
        
        tasks = function() {
            return(private$tasklist)
        }
    ),

    public = list(
        qname = NULL,
        workers = NULL,
        queue = NULL,
        backend = NULL,
        queue_url = NULL,
        backend_url = NULL,

        initialize = function(name='celery', workers,
                              queue="redis://localhost:6379",
                              backend){
            self$queue_url = queue
            self$qname = name
            if (!missing(workers)) {
                self$workers = workers
            } else {
                self$workers = ncores()
            }
            if (!missing(backend)) {
                self$backend_url = backend
            }

            private$rscript = Sys.which('Rscript')[['Rscript']]
            private$wproc = system.file('wprocess', package='rworker')
            private$tasklist = list()

        },

        # Create and register new task 
        task = function(FUN, name, ...) {
            private$tasklist[[name]] = FUN
        },

        # Listen for new messages from message queue
        consume = function(verbose=TRUE, pipe=FALSE) {
            self$register_queue(self$queue_url)
            if (!is.null(self$backend_url)) {
                self$register_backend(self$backend_url)
            }
            self$bootstrap_cluster(self$workers, pipe=pipe)
            log_it(glue::glue('Listening to {self$queue_url}...'),'info')

            tryCatch({
                # main loop
                while (TRUE) {
                    msg = self$queue$pull()
                    # send job to worker
                    if (!is.null(msg)){
                        tereq = ter(msg)
                        tereq$task = self$tasks[[tereq$taskname]]
                        log_it(glue::glue("Received task {tereq$task_id}: {tereq$taskname}"), 'info')
                        self$execute(tereq)
                    }
                    
                    if (!is.null(backend)) {
                        self$update_state()
                    }
                    
                    if (pipe) {
                        lapply(self$pool, function(p) {
                            out <- p$read_output_lines()
                            err <- p$read_error_lines()
                            if (length(out) > 0) print(out)
                            if (length(err) > 0) print(err)
                        })
                    }
                    Sys.sleep(0.1)
                }
            }, finally = self$teardown_cluster())
        },

        execute = function(tereq) {
            send.socket(private$psock,data=tereq)
        },

        update_state = function() {
            report = TRUE
            while (!is.null(report)) {
                report = self$collect_report()
                if (!is.null(report)) {
                    if (report$status == 'PROGRESS') {
                        # class(report) == list
                        log_it(
                            glue('Task {report$task_id} progress: {report$progress}'),'info')
                        
                        self$backend$store_result(report$task_id, report)
                    } else {
                        if (report$status == 'FAILURE') {
                            # class(report) == TER
                            log_it(
                                glue('Task {report$task_id} failed with error: {report$errors}'),'error')
                        } else if (report$status == 'SUCCESS') {
                            log_it(
                                glue('Task {report$task_id} succeeded'), 'success')
                        }
                        
                        self$backend$store_result(report$task_id, report)
                        
                        if (report$has_chain()) {
                            self$trigger_task_callback(report)
                        }
                    }
                }
            }
        },

        collect_report = function() {
            return(receive.socket(private$ssock, dont.wait=TRUE))
        },

        trigger_task_callback = function(tereq) {
            next_task = tereq$next_task()
            nq = queue(self$queue_url, name=next_task$queue)
            nq$connect()
            nq$push(next_task$msg)
        },

        register_queue = function(url) {
            self$queue = queue(url, name=self$qname)
            self$queue$connect()
        },

        register_backend = function(url) {
            self$backend = backend(url)
        },

        # Start processes pool
        start_pool = function(workers, pipe=FALSE) {
            running = lapply(private$workers_list, function(p) {
                            p$is_alive()})
            running = sum(unlist(running))

            if (running == self$workers) {
                warning('Process pool already running')
                return()
            }

            if (pipe) {
                private$workers_list = lapply(1:workers, function(x){
                                    processx::process$new(command=private$rscript,
                                                          args=private$wproc,
                                                          stdout="|", stderr="|")
                                  })
            } else {
                private$workers_list = lapply(1:workers, function(x){
                                    processx::process$new(command=private$rscript,
                                                          args=private$wproc)
                                  })
            }
        },

        # Kill process pool
        kill_pool = function() {
            d = lapply(private$workers_list, function(p){
                    p$kill()
                })
            private$workers_list = list()
        },

        bootstrap_cluster = function(workers, pipe=FALSE) {
            private$context = rzmq::init.context()
            private$bind_ssock()
            self$start_pool(workers, pipe=pipe)
            private$bind_psock()
        },

        teardown_cluster = function() {
            self$kill_pool()
            rzmq::disconnect.socket(private$ssock, "ipc:///tmp/rworkers.sock")
            rzmq::disconnect.socket(private$psock, "ipc:///tmp/rworkerp.sock")
        }

    ),

    private = list(
        rscript = NULL,
        workers_list = NULL,
        tasklist = NULL,
        context = NULL,
        psock = NULL,
        ssock = NULL,
        wproc = NULL,

        bind_psock = function() {
            private$psock = rzmq::init.socket(private$context, 'ZMQ_PUSH')
            rzmq::bind.socket(private$psock, "ipc:///tmp/rworkerp.sock")
        },

        bind_ssock = function() {
            private$ssock = rzmq::init.socket(private$context, 'ZMQ_PULL')
            rzmq::bind.socket(private$ssock, "ipc:///tmp/rworkers.sock")
        }
    )
)

#' @rdname Rworker
#' @export
rworker <- function(name='celery', workers=2,
                    queue='redis://localhost:6379',
                    backend='redis://localhost:6379') {
    return(Rworker$new(name=name, workers=workers, 
                       queue=queue, backend=backend))
}
