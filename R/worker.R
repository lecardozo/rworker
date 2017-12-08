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
        initialize = function() {},

        listen = function() {
            context = init.context()
            socket = init.socket(context, 'ZMQ_PULL')
            bind.socket(socket, 'ipc:///tmp/rworkerp.sock')
            while(TRUE) {
                msg = receive.socket(socket)
                msg$task()
                Sys.sleep(1)
            }
        }


        #update_state = function(status, result=NULL, progress=NULL) {
        #    status = toupper(status)
        #    result = ifelse(is.null(result), "null", result)
        #    progress = ifelse(is.null(progress), "null", progress)
        #    message = glue::glue('{{
        #                            "status":"{status}",
        #                            "result": {result},
        #                            "info":{{
        #                                "progress": {progress},
        #                                "bla":"bla"
        #                            }},
        #                            "task_id":"{self$task_id}",
        #                            "traceback":null,
        #                            "children":null
        #                          }}')
        #    self$status = status
        #    self$backend$SET(self$backend_id, message)
        #},


    ),
)

#' @export
worker = function() {
    return(Worker$new())
}
