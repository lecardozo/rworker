#' @importFrom R6 R6Class
#' @import redux
#' @importFrom jsonlite fromJSON toJSON
#' @importFrom base64enc base64decode
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
#' @param qname The name of the message queue.
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

        initialize = function(qname='celery', workers=1,
                              queue="redis://localhost:6379",
                              backend="redis://localhost:6379"){
            self$queue_url = queue
            self$qname = qname
            self$workers = workers
            self$backend_url = backend

            private$rscript = Sys.which('Rscript')[['Rscript']]
            private$wproc = system.file('wprocess', package='rworker')
            private$tasklist = list()

        },

        # Create and register new task 
        task = function(FUN, name, ...) {
            private$tasklist[[name]] = FUN
        },

        # Listen for new messages from message queue
        consume = function(verbose=TRUE) {
            self$register_queue(self$queue_url)
            self$register_backend(self$backend_url)
            self$bootstrap_cluster(self$workers)

            log_it(
              glue::glue(
                'Listening to {self$queue$provider} in {self$queue$host}...'
              ),
              'info')

            tryCatch({
                while (TRUE) {
                    msg = self$queue$pull()
                    # send job to worker
                    if (!is.null(msg)){
                        procmsg = private$process_msg(msg)
                        tsk = procmsg[['task']]
                        prms = procmsg[['params']]
                        task_id = procmsg[['task_id']]
                        self$execute(task=procmsg[['task']],
                                     params=procmsg[['params']],
                                     task_id=procmsg[['task_id']])
                    }

                    self$update_state()
                    Sys.sleep(0.1)
                }
            }, finally = self$teardown_cluster())
        },

        execute = function(task, params, task_id) {
            send.socket(private$psock,
                        data=list(task=private$tasklist[[task]],
                                  args=params, task_id=task_id))
        },

        update_state = function() {
            report = TRUE
            while (!is.null(report)) {
                report = receive.socket(private$ssock, dont.wait=TRUE)
                if (!is.null(report)) {
                    if (report$status == 'ERROR') {
                        message = list(status=report$status,
                                       result=NULL,
                                       task_id=report$task_id,
                                       traceback=report$errors,
                                       children=NULL)
                        string = glue::glue('Task {report$task_id} failed with error: {report$errors}')
                        log_it(string, 'error')
                    } else if (report$status == 'PROGRESS') {
                        message = list(status=report$status,
                                       result=list(progress=report$progress),
                                       task_id=report$task_id,
                                       traceback=report$errors,
                                       children=NULL)
                        string = glue::glue('Task {report$task_id} progress: {report$progress}')
                        log_it(string, 'success')

                    } else if (report$status == 'SUCCESS') {
                        message = list(status=report$status,
                                       result=TRUE,
                                       task_id=report$task_id,
                                       traceback=report$errors,
                                       children=NULL)
                        string = glue::glue('Task {report$task_id} succeeded')
                        log_it(string, 'success')
                    }
                    message = jsonlite::toJSON(message, auto_unbox=TRUE, null='null')
                    print(message)
                    self$backend$SET(glue::glue('celery-task-meta-{report$task_id}'),message)
                }
            }
        },

        register_queue = function(url) {
            parsed = parse_url(url)
            self$queue = do.call(Queue$new, append(parsed, 
                                                   list(qname=self$qname)))
        },

        register_backend = function(url) {
            backend = parse_url(url)
            if (backend[["provider"]] == "redis") {
                return(redux::hiredis(host=backend[["host"]],
                                      port=backend[["port"]]))
            }
        },

        # Start processes pool
        start_pool = function(workers) {
            running = lapply(private$workers_list, function(p) {
                            p$is_alive()})
            running = sum(unlist(running))

            if (running == self$workers) {
                warning('Process pool already running')
                return()
            }
            private$workers_list = lapply(1:workers, function(x){
                                processx::process$new(command=private$rscript,
                                                      args=private$wproc,
                                                      stdout='|',stderr='|')
                              })
        },

        # Kill process pool
        kill_pool = function() {
            d = lapply(private$workers_list, function(p){
                    p$kill()
                })
            private$workers_list = list()
        },

        bootstrap_cluster = function(workers) {
            private$context = rzmq::init.context()
            private$bind_ssock()
            self$start_pool(workers)
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

        process_msg = function(msg) {
            action = jsonlite::fromJSON(msg)
            task = action$headers$task
            task_id = action$headers$id
            log_it(glue::glue("Received task {task_id}: {task}"), 'info')
            params = jsonlite::fromJSON(rawToChar(base64enc::base64decode(action$body)))[[2]]
            return(list(task=task, params=params, task_id=task_id))
        },

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
rworker <- function(qname='celery', workers=2,
                    queue='redis://localhost:6379',
                    backend='redis://localhost:6379') {
    return(Rworker$new(qname=qname, workers=workers, 
                       queue=queue, backend=backend))
}
