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
            self$backend = backend

            private$rscript = Sys.which('Rscript')[['Rscript']]
            private$wproc = system.file('wprocess', package='rworker')
            private$tasklist = list()
            
            # ZMQ initialization
            self$set_ssock()
            self$start_pool(workers)
            self$set_psock()
            
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

                 # adds new jobs
                if (!is.null(msg)){
                    if (self$available()) {
                        procmsg = private$process_msg(msg)
                        tsk = procmsg[['task']]
                        prms = procmsg[['params']]
                        self$execute(tsk)
                    } else {
                        # push task back into the queue
                        self$queue$push(msg)
                    }
                }
                selg$update_status()
                Sys.sleep(0.1)
            }
        },

        execute = function(task, ...) {
            send.socket(private$psock,
                        data=list(task=private$tasklist[[task]], 
                                  args=list(...)))
        },

        update_status = function() {
            msg = receive.socket(private$ssock)
            print(msg)
        },

        available = function() {
            return(TRUE) 
        },

        set_psock = function() {
            private$pcontext = rzmq::init.context()
            private$psock = rzmq::init.socket(private$pcontext, 'ZMQ_PUSH')
            rzmq::connect.socket(private$psock, "ipc:///tmp/rworkerp.sock")
        },

        set_ssock = function() {
            private$scontext = rzmq::init.context()
            private$ssock = rzmq::init.socket(private$scontext, 'ZMQ_PULL')
            rzmq::connect.socket(private$ssock, "ipc:///tmp/rworkers.sock")
        }
    ),

    private = list(
        rscript = NULL,
        workers_list = NULL,
        workers_status = NULL,
        tasklist = NULL,
        pcontext = NULL,
        scontext = NULL,
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
