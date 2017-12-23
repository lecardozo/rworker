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
#' The TER object represents the request sent by Celery.
#'
#' @rdname TER
#' @name TER
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

        has_chain = function() {
            if (length(self$chain) > 0) {
                return(TRUE)
            } else {
                return(FALSE)
            }
        },

        next_task = function() {
            if (!is.null(self$chain)) {
                next_task_info =  self$chain$chain[[1]]
                # BODY
                # args
                self$body[[1]] = next_task_info$args
                # kwargs
                self$body[[2]] = next_task_info$kwargs
                # chain
                if (length(self$chain$chain[-1]) == 0) {
                    self$body[[3]] = list()
                } else {
                    self$body[[3]] = self$chain$chain[-1] 
                }
                
                # HEADERS
                self$headers$parent_id = self$task_id
                self$headers$lang = 'r'
                self$headers$kwargsrepr = private$repr(self$body[[2]])
                self$headers$argsrepr = private$repr(self$body[[1]])
                self$headers$id = next_task_info$options$task_id
                self$headers$task = next_task_info$task
                self$headers$eta = list(NULL)

                # PROPERTIES
                self$properties$correlation_id = self$headers$id
                if (!is.null(next_task_info$options$queue)) {
                    self$properties$delivery_info$routing_key = next_task_info$options$queue
                }
                return(list(queue=self$properties$delivery_info$routing_key,
                            msg=self$tojson()))

            } else {
                return(NULL)
            }
        },

        tojson = function() {
            body = base64enc::base64encode(
                       charToRaw(
                           jsonlite::toJSON(self$body, auto_unbox=T)
                        )
                   )
            msg = list(body=body, headers=self$headers, 
                       properties=self$properties, 
                       "content-type"=self$content_type,
                       "content-encoding"=self$content_encoding)
            return(jsonlite::toJSON(msg, auto_unbox=T))
        }
    ),

    private = list(
        parse_msg = function(msg) {
            request = jsonlite::fromJSON(msg)
            self$headers = request$headers
            self$body = jsonlite::fromJSON(
                            rawToChar(base64enc::base64decode(request$body))
                        , simplifyDataFrame=FALSE)
            self$properties = request$properties
            self$content_encoding = request[['content-encoding']]
            self$content_type = request[['content-type']]
        },

        repr = function(obj) {
            return(as.character(toJSON(obj, auto_unbox=T, null='null')))
        }
    )

)

ter = function(msg) {
    return(TER$new(msg))
}
