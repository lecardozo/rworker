#' @importFrom R6 R6Class
#' @import redux
#' @importFrom jsonlite fromJSON toJSON
#' @importFrom base64enc base64decode
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



#' Tasks object
#'
#' This object is stores all the functions that can be called by the
#' message broker.
#'
#' @section Usage:
#' \preformatted{
#' dummy1 <- function(){Sys.sleep(5); print('dummy1')}
#' dummy2 <- function(){Sys.sleep(5); print('dummy2')}
#'
#' tasks <- Tasks$new(tasklist=list(dummy1, dummy2)
#'
#' }
#'
#' @section Arguments:
#' \describe{
#'     \item{tasks:}{A \code{Tasks} object.}
#'     \item{tasklist:}{List object containing all functions.}
#'     \item{src:}{Character. Name of file containing functions.}
#' }
#' @name Tasks
NULL

#' @export
Tasks <- R6::R6Class(
    'Tasks',
    public = list(
        tasklist = NULL,
        src = NULL,
        tmp = NULL,
        initialize = function(tasklist, src) {
            if (!missing(tasklist)) {
                self$src = tmpsrc(tasklist)
                self$tasklist = tasklist
                self$tmp = TRUE
            } else if (!missing(src)) {
                self$src = src
                tmpenv = new.env()
                source(src, envir=tmpenv)
                self$tasklist = sapply(tmpenv, function(f) f)
                self$tmp = FALSE
            } else {
                stop('Must provide tasklist or src')
            }
        }
    )
)

#' Worker object
#'
#' This object is responsible for running tasks consumed from the
#' message broker. It uses processx to spawn background processes.
#'
#' @section Usage:
#' \preformatted{
#' dummy1 <- function(){Sys.sleep(5); print('dummy1')}
#' dummy2 <- function(){Sys.sleep(5); print('dummy2')}
#'
#' tasks <- Tasks$new(tasklist=list(dummy1, dummy2)
#'
#' worker1 <- Worker$new(FUN='dummy1',tasks=tasks)
#' worker2 <- Worker$new(FUN='dummy2',tasks=tasks)
#' }
#'
#' @section Arguments:
#' \describe{
#'     \item{worker:}{A \code{Worker} object.}
#'     \item{FUN:}{Character. Name of the function to be executed in background.}
#'     \item{params:}{List object mapping parameters to values.
#'                   E.g. list('param'='value').}
#'     \item{tasks:}{A \code{Tasks} object.}
#' }
#' @name Worker
NULL

#' @export
Worker <- R6::R6Class(
    'Worker',
    public = list(
        FUN = NULL,
        params = NULL,
        tasks = NULL,
        process = NULL,

        initialize = function(FUN, params, tasks) {
            if (missing(FUN)) { stop('Must provide FUN') }
            if (missing(tasks)) { stop('Must provide Tasks object') }
            if (missing(params)) { params = ''  }
            if (class(params) == 'list') {
                params = paste(paste0(names(params),'=',params), collapse=',')
            }
            args = sprintf('source("%s"); %s(%s)', tasks$src, FUN, params) 
            self$process = processx::process$new(command='/usr/bin/Rscript',
                                           args=c('-e', args),
                                           stdout='|', stderr='|',
                                           echo_cmd=TRUE)
        },

        is_running = function() {
            return(self$process$is_alive())
        },

        has_output = function() {
            return(self$process$is_incomplete_output())
        },

        get_output = function() {
            return(self$process$read_output_lines())
        },

        has_error = function() {
            return(self$process$is_incomplete_error())
        },

        get_error = function() {
            return(self$process$read_error_lines())
        }
    ),
)

#' Consumer object
#'
#' The Consumer object consumes messages sent to the message
#' broker and spawns Workers to run functions in background.
#'
#' @section Usage:
#' \preformatted{
#' dummy1 <- function(){Sys.sleep(5); print('dummy1')}
#' dummy2 <- function(){Sys.sleep(5); print('dummy2')}
#'
#' tasks <- Tasks$new(tasklist=list(dummy1, dummy2)
#' queue <- Queue$new()
#' 
#' consumer <- Consumer$new(queue=queue, nworkers=2, tasks=tasks)
#' consumer$consume()
#' }
#'
#' @section Arguments:
#' \describe{
#'     \item{consumer:}{A \code{Consumer} object.}
#'     \item{nworkers:}{Numeric. Maximum number of background workers.
#'                     Default is 1.}
#'     \item{tasks:}{A \code{Tasks} object.}
#' }
#' @name Consumer
NULL

#' @export
Consumer <- R6::R6Class(
    'Consumer',
    public = list(
        queue = NULL,
        nworkers = NULL,
        tasks = NULL,
        initialize = function(queue, tasks, nworkers=1) {
            private$workerlist = list()
            if(missing(queue)){
                stop('Must provide Queue object')
            } else {
                self$queue = queue
            }
            
            if(missing(tasks)){
                stop('Must provide Tasks object')
            } else {
                self$tasks = tasks
            }
            
        },

        consume = function(verbose=TRUE) {
            if (verbose) { cat(sprintf('Listening to %s in %s ...\n',
                                        self$queue$provider, self$queue$host)) }

            tryCatch({ 
                while (TRUE) {
                    msg = self$queue$pull()

                    # adds new jobs
                    if (!is.null(msg)){
                        if (length(private$workerlist) < self$nworkers) {
                            processed_msg <- private$.proc_msg(msg)
                            worker = Worker$new(FUN=processed_msg$task,
                                                params=processed_msg$params,
                                                tasks=self$tasks)
                        } else {
                            # push task back into the queue
                            self$queue$push(msg)
                        }
                    }
                    # check process status
                    private$.update_workerlist()
                    Sys.sleep(2)
                }
           }, interrupt=function(){
                                    if (self$tasks$tmp) {
                                        unlink(self$tasks$src)
                                    }
                                  }
           )
        }
    ),

    private = list(
        workerlist = NULL,
        .proc_msg = function(msg) {
            action <- jsonlite::fromJSON(msg)
            task <- action$headers$task
            task_id <- action$headers$id
            cat('Received task', task_id, ':', task, '\n')
            params <- jsonlite::fromJSON(rawToChar(base64enc::base64decode(action$body)))[[2]]
            return(list(task=task, params=params))
        },

        .update_workerlist = function() {
            if (length(private$workerlist) > 0 ){
                to_remove = sapply(private$workerlist, function(worker){
                    if (!worker$is_running()) {
                        return(FALSE)
                    } else {
                        if (worker$has_output()){
                            out = worker$get_output()
                            if (length(out) != 0 && parameters[['verbose']]) {
                                sapply(out, function(x) cat('INFO: ',x,'\n'))
                            }
                        }
                        if (worker$has_error()){
                            err = worker$get_error()
                            if (length(err) != 0 && parameters[['verbose']]) {
                                cat('ERROR: ',err,'\n')
                            }
                        }
                        return(TRUE)
                    }
                }, simplify=TRUE)
                private$workerlist = private$workerlist[to_remove]
            }
        }
    )
)

#' @keywords internal
tmpsrc <- function(fnlist) {
    tmp <- tempfile(pattern='rworker')
    for (i in seq_along(fnlist)) {
        fname <- names(fnlist[i])
        fun <- capture.output(fnlist[[i]])
        fun <- fun[!grepl('<bytecode|environment.*>', fun)]
        write(paste0(fname, '<-', fun), tmp, append=TRUE)
    }
    return(tmp)
}
