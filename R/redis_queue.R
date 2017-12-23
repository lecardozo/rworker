#' @importFrom R6 R6Class
#' @import redux
NULL

#' RedisQueue object
#'
#' This object establishes an interface for Redis as a message broker
#'
#' @section Usage:
#' ```
#' queue <- RedisQueue$new(host='localhost',
#'                         port=6379, name='celery')
#' msg <- queue$pull()
#' queue$push(msg)
#' ```
#' @param name The name of the queue.
#' @param host Message broker instance address.
#' @param port Message broker port.
#'
#' @name RedisQueue
NULL

#' @export 
RedisQueue <- R6::R6Class(
    'RedisQueue',
    public = list(
        host = NULL,
        port = NULL,
        name = NULL,

        initialize = function(name='celery', host='localhost', port=6379) {
            self$host = host
            self$port = port
            if(missing(name)) {
                   stop('Must provide Queue name')
            } else {
                self$name = name
            }
        },

        pull = function() {
            msg = private$channel$LPOP(self$name)
            return(msg)
        },

        push = function(msg) {
            private$channel$LPUSH(self$name, msg)
        },

        connect = function() {
            private$channel = redux::hiredis(host=self$host,
                                             port=self$port)
        }
    ),

    private = list(
        channel = NULL
    )
)
