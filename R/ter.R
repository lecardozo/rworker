#' @importFrom R6 R6Class
#' @import redux
#' @importFrom jsonlite fromJSON toJSON
#' @importFrom base64enc base64decode
#' @import glue
#' @import rzmq
NULL

#' Task Execution Request
#'
#' @section Usage:
#' ```
#' task_request <- ter(message)
#' ```
#' 
#' @section Details:
#'
#' @rdname TER
#' @name TER
#' @examples
#'
NULL

#' @export
TER <- R6::R6Class(
    'TER',
    lock_objects=FALSE,
    active = list(
        taskname = function() self$headers$task,
        task_id = function() self$headers$id,
        args = function() self$body[[1]],
        kwargs = function() self$body[[2]],
        chain = function() self$body[[3]],
        chord = function() self$body[[4]]
    ),

    public = list(
        body=NULL,
        headers=NULL,
        properties=NULL,
        content_encoding=NULL,
        content_type=NULL,
        task=NULL,
        status='PENDING',
        progress=NULL,
        warnings=character(0),
        errors=character(0),
        
        initialize = function(msg) {
            private$parse_msg(msg)
        },

        next_task = function() {
            if (is.null(self$chain)) {
                return(NULL)
            } else {
                return(NULL)
            }
        }
    ),

    private = list(
        parse_msg = function(msg) {
            request = jsonlite::fromJSON(msg)
            self$headers = request$headers
            self$body = jsonlite::fromJSON(rawToChar(base64enc::base64decode(request$body)))
        }
    )

)

ter = function(msg) {
    return(TER$new(msg))
}
