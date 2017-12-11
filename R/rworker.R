#' @importFrom R6 R6Class
#' @import redux
#' @importFrom jsonlite fromJSON toJSON
#' @importFrom base64enc base64decode
#' @import futile.logger
#' @import glue
#' @import crayon
#' @import rzmq
NULL

#' Rworker object
#'
#' The Rworker object consumes messages sent to the message
#' broker and sends tasks to be executed in background by the
#' process pool.
#'
#' @section Usage:
#' \preformatted{
#'  \dontrun{ 
#'        # Create rworker instance
#'        rwork <- rworker()
#'
#'        # Register task
#'        (function() { 
#'            Sys.sleep(5)
#'        }) %>% rwork$task(name='long_running_task')
#'
#'        # Send task for background execution
#'        rwork$execute('long_running_task')
#'
#'        # Listen to messages from message queue
#'        rwork$consume()
#'  }
#'
#' }
#'
#' @section Arguments:
#' \describe{
#'     \item{qname:}{The name of the message queue.}
#'     \item{workers:}{The number of background worker processes.}
#'     \item{queue:}{A url string of type "provider://host:port".}
#'     \item{backend:}{A url string of type "provider://host:port".}
#' }
#' @name Rworker 
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
            self$connect_ssock()
            self$start_pool(workers)
            self$bind_psock()
            
        },

        # Start processes pool
        start_pool = function(workers) {
            private$workers_list = lapply(1:workers, function(x){
                                processx::process$new(command=private$rscript,
                                                      args=private$wproc,
                                                      stdout='|',stderr='|')
                              })
            private$workers_status = rep('LISTENING', workers)
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
            self$start_pool(self$workers)
            log_it(
              glue::glue(
                'Listening to {self$queue$provider} in {self$queue$host}...'
              ),
              'info')

            while (TRUE) {
                msg = self$queue$pull()

                # send job to worker
                if (!is.null(msg)){
                    procmsg = private$process_msg(msg)
                    tsk = procmsg[['task']]
                    prms = procmsg[['params']]
                    task_id = procmsg[['task_id']]
                    self$execute(task=procmsg[['task']],
                                 args=procmsg[['params']],
                                 task_idprocmsg[['task_id']])
                }

                self$update_status()
                Sys.sleep(0.1)
            }
        },

        execute = function(task, params, task_id) {
            send.socket(private$psock,
                        data=list(task=private$tasklist[[task]],
                                  args=params), task_id=task_id)
        },

        update_status = function() {
            report = receive.socket(private$ssock)
            message = glue::glue('{{
                                    "status":"{report$status}",
                                    "result":null,
                                    "task_id":"{report$task_id}",
                                    "traceback":"{report$errors}",
                                    "children":null
                                  }}')
            self$backend$SET(glue::glue('celery-task-meta-{report$task_id}'))
        },

        register_backend = function(url) {
            backend = parse_url(url)
            if (backend[["provider"]] == "redis") {
                return(redux::hiredis(host=backend[["host"]],
                                      port=backend[["port"]]))
            }
        },

        connect_psock = function() {
            private$psock = rzmq::init.socket(private$context, 'ZMQ_PUSH')
            rzmq::connect.socket(private$psock, "ipc:///tmp/rworkerp.sock")
        },

        bind_ssock = function() {
            private$ssock = rzmq::init.socket(private$context, 'ZMQ_PULL')
            rzmq::bind.socket(private$ssock, "ipc:///tmp/rworkers.sock")
        }
    ),

    private = list(
        rscript = NULL,
        workers_list = NULL,
        workers_status = NULL,
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
        }
    )
)


#' @export
rworker <- function(qname='celery', workers=2,
                    queue='redis://redis:6379',
                    backend='redis://redis:6379') {
    return(Rworker$new(qname=qname, workers=workers, 
                       queue=queue, backend=backend))
}
