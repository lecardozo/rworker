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

        initialize = function() {
            private$context = rzmq::init.context()
            private$psock = rzmq::init.socket(private$context, 'ZMQ_PULL')
            private$ssock = rzmq::init.socket(private$context, 'ZMQ_PUSH')
            rzmq::connect.socket(private$psock, "ipc:///tmp/rworkerp.sock")
            rzmq::connect.socket(private$ssock, "ipc:///tmp/rworkers.sock")
        },

        listen = function() { 
            while(TRUE) {
                tereq = receive.socket(private$psock)
                private$inject_progress(tereq)

                tryCatch({
                    suppressWarnings({
                        withCallingHandlers({
                            do.call(tereq$task, tereq$kwargs)
                        }, warning=function(w) {
                            tereq$warns=c(tereq$warnings,
                                            gsub('\n', ';', as.character(w)))
                        })
                    })
                },
                    error=function(e) {tereq$errors=gsub('\n', ';', as.character(e))},
                    finally=self$report(tereq))
            }
        },

        report = function(tereq) {
            if (length(tereq$errors) > 0) {
                tereq$status = 'FAILURE'
            } else {
                tereq$status = 'SUCCESS'
            }
            send.socket(private$ssock, data=tereq)
        }
    ),

    private = list(
        context = NULL,
        psock = NULL,
        ssock = NULL,

        inject_progress = function(tereq) {
            expr <- substitute({
                task_progress <- function(pg) {
                    rzmq::send.socket(socket,
                                      data=list(status='PROGRESS',
                                                progress=pg,
                                                errors=NULL,
                                                warnings=NULL,
                                                task_id=id))
                }
            }, list(id=tereq$task_id, socket=private$ssock))
            body(tereq$task) <- as.call(append(as.list(body(tereq$task)), expr, 1))
        }
    ),
)

#' @rdname Worker
#' @export
worker = function() {
    return(Worker$new())
}
