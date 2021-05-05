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
#'                         port=6379, name='celery',
#'                         password=NULL, db=0)
#' msg <- queue$pull()
#' queue$push(msg)
#' ```
#' @param name The name of the queue.
#' @param host Message broker instance address.
#' @param port Message broker port.
#' @param username Ignored (included for consistency with other non-Redis queues)
#' @param password Redis password
#' @param db Database number
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
        username = NULL,
        password = NULL,
        db = NULL,

        initialize = function(name='celery', host='localhost', port=6379, username=NULL, password=NULL, db=0) {
            self$host = host
            self$port = port
            self$username = username
            self$password = password
            self$db = db
            if (is.na(as.numeric(db))) {
                stop("db parameter must be numeric")
            }
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
                                             port=self$port, 
                                             password=self$password, 
                                             db=self$db)
        }
    ),

    private = list(
        channel = NULL
    )
)
