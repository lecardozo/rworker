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
#' \dontrun{
#' \preformatted{
#'  w <- worker()
#' 
#'  w$listen()
#' }
#' }
#'
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
                    error=function(e) {self$errors=gsub('\n', ';', as.character(e))},
                    warning=function(w) {self$warnings=gsub('\n', ';', as.character(w))},
                    finally=self$report())
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
        ssock = NULL
    ),
)

#' @name Worker
#' @export
worker = function() {
    return(Worker$new())
}
