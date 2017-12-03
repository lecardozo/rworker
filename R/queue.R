#' @importFrom R6 R6Class
#' @import redux
NULL

#' Queue object
#'
#' This object establishes an interface for different
#' message brokers using the AMQP Protocol.
#'
#' @section Usage:
#' \preformatted{queue <- Queue$new(provider='redis', host='localhost',
#'                                    port=6379, name='celery')
#' msg <- queue$pull()
#' queue$push(msg)
#' }
#'
#' @section Arguments:
#' \describe{
#'     \item{queue:}{A \code{Queue} object.}
#'     \item{provider:}{Character scalar referring to the message broker provider.
#'                     \code{'redis'} is the only available option until now.}
#'     \item{host:}{Character. Message broker instance address.}
#'     \item{port:}{Numeric. Message broker port.}
#' }
#' @name Queue
NULL

#' @export 
Queue <- R6::R6Class(
    'Queue',
    public = list(
        provider = NULL,
        host = NULL,
        port = NULL,
        name = NULL,

        initialize = function(name, provider='redis', host='localhost',
                              port=6379) {
            self$host = host
            self$port = port
            if(missing(name)) {
                   stop('Must provide Queue name')
            } else {
                self$name = name
            }
            if (provider == 'redis') {
                self$provider = provider
                private$channel = redux::hiredis(host=self$host,
                                                 port=self$port)
            }
        },

        pull = function() {
            if (self$provider == 'redis') {
                msg = private$channel$LPOP(self$name)
            }
            return(msg)
        },

        push = function(msg) {
            if (self$provider == 'redis') {
                private$channel$LPUSH(self$name, msg)
            }
        }
    ),

    private = list(
        channel = NULL
    )
)
