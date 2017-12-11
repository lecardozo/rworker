#' @importFrom R6 R6Class
#' @import redux
#' @import glue
#' @import rzmq
NULL

#' Worker object
#'
#' This object is responsible for running tasks consumed from the
#' message broker. It uses processx to spawn background processes.
#'
#' @section Usage:
#' \preformatted{
#'  w <- worker()
#' 
#'  \dontrun{w$listen()}
#' }
#'
#' @section Arguments:
#' \describe{
#'     \item{worker:}{A \code{Worker} object.}
#'     \item{FUN:}{Character. Name of the function to be executed in background.}
#'     \item{params:}{List object mapping parameters to values.
#'                   E.g. list('param'='value').}
#'     \item{backend_url:}{A url string of the following format: "provider://host:port".}
#'     \item{id:}{A unique identifier for the task.}
#' }
#' @name Worker
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
            rzmq::bind.socket(private$psock, "ipc:///tmp/rworkerp.sock")
            rzmq::connect.socket(private$ssock, "ipc:///tmp/rworkers.sock")
        },

        listen = function() {
            while(TRUE) {
                msg = receive.socket(private$psock)
                self$current_task = msg$task_id

                tryCatch({ do.call(msg$task, msg$args) },
                    error=function(e) { self$errors = e },
                    warning=function(w) { self$warnings = w },
                    finally=self$report())
            }
        },

        report = function() {
            if (!is.null(self$errors)) {
                status = 'FAILED'
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
        ssock = NULL
    ),
)

#' @export
worker = function() {
    return(Worker$new())
}
