#' @importFrom R6 R6Class
#' @import redux
#' @import glue
NULL

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
#' tasks <- Tasks$new(tasklist=list(dummy1=dummy1, dummy2=dummy2))
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
#'     \item{backend_url:}{A url string of the following format: "provider://host:port".}
#'     \item{id:}{A unique identifier for the task.}
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
        task_id = NULL,
        process = NULL,
        interpreter = NULL,
        backend = NULL,
        backend_id = NULL,
        status = NULL,

        initialize = function(FUN, params, tasks, 
                              backend_url='redis://localhost:6379',
                              id) {
            if (missing(FUN)) { stop('Must provide FUN') }
            if (missing(tasks)) { stop('Must provide Tasks object') }
            if (missing(params)) { params = ''  }
            if (class(params) == 'list') {
                if (length(params) > 0) {
                    params = paste(paste0(names(params),'=',params), collapse=',')
                } else {
                    params = ""
                }
            }
            self$tasks = tasks         
            self$task_id = id
            self$register_backend(backend_url)
            self$interpreter = Sys.which('Rscript')[[1]]
            self$status = 'PENDING'

            args = glue::glue('source("{self$tasks$src}"); {FUN}({params})')
            self$process = processx::process$new(command=self$interpreter,
                                           args=c('-e', args),
                                           stdout='|', stderr='|') 
        },

        update_state = function(status, result=NULL, progress=NULL) {
            status = toupper(status)
            result = ifelse(is.null(result), "null", result)
            progress = ifelse(is.null(progress), "null", progress)
            message = glue::glue('{{
                                    "status":"{status}",
                                    "result": {result},
                                    "info":{{
                                        "progress": {progress},
                                        "bla":"bla"
                                    }},
                                    "task_id":"{self$task_id}",
                                    "traceback":null,
                                    "children":null
                                  }}')
            self$status = status
            self$backend$SET(self$backend_id, message)
        },

        register_backend = function(url) {
            backend = private$parse_url(url)
            if (backend[["provider"]] == "redis") {
                qname = glue::glue("celery-task-meta-{self$task_id}")
                self$backend_id = qname
                self$backend = redux::hiredis(host=backend[["host"]],
                                              port=backend[["port"]])
            }
        },

        failed = function() {
            if (self$status=="FAILED") {
                return(TRUE)
            }
            return(FALSE)
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

    private = list(
        parse_url = function(url) {
            parsed = as.list(stringr::str_match(url, "(.*)://(.*):(\\d*)"))
            parsed = parsed[-1]
            names(parsed) <- c("provider", "host", "port")
            return(parsed)
        }
    ),
)
