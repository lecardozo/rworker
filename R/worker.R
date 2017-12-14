#' @importFrom R6 R6Class
#' @import redux
#' @import glue
#' @import rzmq
NULL

#' Worker object
#'
#' This object listens for tasks sent by some Rworker instance.
#' After running, the status of the execution is sent back 
#' to the Rworker instance.
#'
#' @section Usage:
#' ```
#' w <- worker()
#' ```
#' 
#' @section Details:
#' `$listen()` listens for tasks sent by the Rworker instance
#' `$report()` returns the task execution state to Rworker instance 
#' @rdname Worker
#' @name Worker
#' @examples
#' \dontrun{
#' w <- worker()
#' w$listen()
#' }
NULL

#' @export
Worker <- R6::R6Class(
    'Worker',
    public = list(
        warnings = NULL,
        errors = NULL,
        current_task = NULL,

        initialize = function() {
            private$context = rzmq::init.context()
            private$psock = rzmq::init.socket(private$context, 'ZMQ_PULL')
            private$ssock = rzmq::init.socket(private$context, 'ZMQ_PUSH')
            rzmq::connect.socket(private$psock, "ipc:///tmp/rworkerp.sock")
            rzmq::connect.socket(private$ssock, "ipc:///tmp/rworkers.sock")
        },

        listen = function() {
            while(TRUE) {
                msg = receive.socket(private$psock)
                self$current_task = msg$task_id
                task = private$inject_progress(msg$task, msg$task_id, private$ssock)

                tryCatch({ 
                    withCallingHandlers({
                       do.call(task, msg$args)
                    }, warning=function(w) {self$warnings=gsub('\n', ';', as.character(w))}) 
                },
                    error=function(e) {self$errors=gsub('\n', ';', as.character(e))},
                    finally=self$report()
                )
            }
        },

        report = function() {
            if (!is.null(self$errors)) {
                status = 'ERROR'
            } else {
                status = 'SUCCESS'
            }

            send.socket(private$ssock,
                        data=list(status=status,
                                  errors=self$errors,
                                  warnings=self$warnings,
                                  task_id=self$current_task))
            self$errors = NULL
            self$warnings = NULL
            self$current_task = NULL
        }
    ),

    private = list(
        context = NULL,
        psock = NULL,
        ssock = NULL,

        inject_progress = function(fn, current_task, socket) {
            expr <- substitute({
                task_progress <- function(pg) {
                    rzmq::send.socket(socket,
                                data=list(status='PROGRESS',
                                          progress=pg,
                                          errors=NULL,
                                          warnings=NULL,
                                          task_id=current_task))
                }
            }, list(current_task=current_task, socket=socket))
            body(fn) <- as.call(append(as.list(body(fn)), expr, 1))
            return(fn)
        }
    ),
)

#' @rdname Worker
#' @export
worker = function() {
    return(Worker$new())
}
