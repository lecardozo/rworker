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
#' `$task()` registers function as task to be remotelly executed
#'
#' `$tasks()` returns all registered tasks
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
        }
    ),

    public = list(
        qname = NULL,
        workers = NULL,
        queue = NULL,
        backend = NULL,

        initialize = function(qname='celery', workers=1,
                              queue="redis://localhost:6379",
                              backend="redis://localhost:6379"){
            parsed = parse_url(queue)
            self$queue = do.call(Queue$new, append(parsed, list(qname=qname)))
            self$qname = qname
            self$workers = workers
            self$backend = self$register_backend(backend)

            private$rscript = Sys.which('Rscript')[['Rscript']]
            private$wproc = system.file('wprocess', package='rworker')
            private$tasklist = list()

            # ZMQ initialization
            private$context = rzmq::init.context()
            private$bind_ssock()
            self$start_pool(workers)
            private$connect_psock()

        },

        # Start processes pool
        start_pool = function(workers) {
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

        # Create and register new task 
        task = function(FUN, name, ...) {
            private$tasklist[[name]] = FUN
        },

        # Return registered tasks
        tasks = function() {
            return(private$tasklist)
        },

        # Listen for new messages from message queue
        consume = function(verbose=TRUE) {
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
            }, finally = self$kill_pool())
        },

        execute = function(task, params, task_id) {
            send.socket(private$psock,
                        data=list(task=private$tasklist[[task]],
                                  args=params, task_id=task_id))
        },

        update_state = function() {
            report = receive.socket(private$ssock, dont.wait=TRUE)
            if (!is.null(report)) {
                message = list(status=report$status,
                               result=NULL,
                               task_id=report$task_id,
                               traceback=report$errors,
                               children=NULL)
                message = jsonlite::toJSON(message, auto_unbox=TRUE, null='null')
                if (report$status == 'ERROR') {
                    string = glue::glue('Task {report$task_id} failed with error: {report$errors}')
                    log_it(string, 'error')
                } else if (report$status == 'SUCCESS') {
                    string = glue::glue('Task {report$task_id} succeeded')
                    log_it(string, 'success')
                }
                self$backend$SET(glue::glue('celery-task-meta-{report$task_id}'),
                                 message)
            }
        },

        register_backend = function(url) {
            backend = parse_url(url)
            if (backend[["provider"]] == "redis") {
                return(redux::hiredis(host=backend[["host"]],
                                      port=backend[["port"]]))
            }
        }

    ),

    private = list(
        rscript = NULL,
        workers_list = NULL,
        tasklist = NULL,
        context = NULL,
        psock = NULL,
        ssock = NULL,

        process_msg = function(msg) {
            action = jsonlite::fromJSON(msg)
            task = action$headers$task
            task_id = action$headers$id
            log_it(glue::glue("Received task {task_id}: {task}"), 'info')
            params = jsonlite::fromJSON(rawToChar(base64enc::base64decode(action$body)))[[2]]
            return(list(task=task, params=params, task_id=task_id))
        },

        connect_psock = function() {
            private$psock = rzmq::init.socket(private$context, 'ZMQ_PUSH')
            rzmq::connect.socket(private$psock, "ipc:///tmp/rworkerp.sock")
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
