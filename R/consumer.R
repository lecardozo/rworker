#' @importFrom R6 R6Class
#' @import redux
#' @importFrom jsonlite fromJSON toJSON
#' @importFrom base64enc base64decode
#' @import futile.logger
#' @import glue
#' @import crayon
NULL

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
#' tasks <- Tasks$new(tasklist=list(dummy1=dummy1, dummy2=dummy2))
#' queue <- Queue$new()
#' 
#' consumer <- Consumer$new(queue=queue, nworkers=2, tasks=tasks)
#' consumer$consume()
#' }
#'
#' @section Arguments:
#' \describe{
#'     \item{consumer:}{A \code{Consumer} object.}
#'     \item{tasks:}{A \code{Tasks} object.}
#'     \item{queue:}{A \code{Queue} object.}
#'     \item{backend_url:}{A url string of the following format: "provider://host:port".}
#'     \item{nworkers:}{Numeric. Maximum number of background workers.
#'                     Default is 1.}
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
        backend_url=NULL,

        initialize = function(queue, tasks, nworkers=1,
                              backend_url="redis://localhost:6379"){
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

            self$nworkers = nworkers
            self$backend_url = backend_url
            
        },

        consume = function(verbose=TRUE) {
            if (verbose) { futile.logger::flog.info(
                                crayon::bold(glue::glue('Listening to {self$queue$provider} in {self$queue$host}...'))) }

            tryCatch({ 
                while (TRUE) {
                    msg = self$queue$pull()

                    # adds new jobs
                    if (!is.null(msg)){
                        if (length(private$workerlist) < self$nworkers) {
                            processed_msg = private$.proc_msg(msg)
                            worker = Worker$new(FUN=processed_msg$task,
                                                params=processed_msg$params,
                                                tasks=self$tasks,
                                                backend_url=self$backend_url,
                                                id=processed_msg$task_id)
                            private$workerlist = append(private$workerlist, worker)
                        } else {
                            # push task back into the queue
                            self$queue$push(msg)
                        }
                    }
                    # check process status
                    private$.update_workerlist()
                    Sys.sleep(0.1)
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
            action = jsonlite::fromJSON(msg)
            task = action$headers$task
            task_id = action$headers$id
            flog.info(crayon::blue(glue("Received task {task_id}: {task}")))
            params = jsonlite::fromJSON(rawToChar(base64enc::base64decode(action$body)))[[2]]
            return(list(task=task, params=params, task_id=task_id))
        },

        .update_workerlist = function() {
            if (length(private$workerlist) > 0 ){
                to_remove = sapply(private$workerlist, function(worker){
                    if (worker$has_output()){
                        out = worker$get_output()
                        if (length(out) != 0) {
                            worker$update_state(status='PROGRESS')
                            sapply(out, function(x) futile.logger::flog.info(
                                                        crayon::green(glue::glue("{worker$task_id}:{x}"))))
                        }
                    }
                    if (worker$has_error()){
                        err = worker$get_error()
                        if (length(err) != 0 && err !="") {
                            worker$update_state(status='FAILED')
                            futile.logger::flog.error(crayon::red(glue::glue("{worker$task_id}: {err}")))
                        }
                    }
                    
                    if (!worker$is_running()) {
                        if (!worker$failed()) {
                            worker$update_state(status='SUCCESS')
                        } 
                        return(FALSE)
                    } else {
                        return(TRUE)
                    }
                }, simplify=TRUE)
                private$workerlist = private$workerlist[to_remove]
            }
        }
    )
)
