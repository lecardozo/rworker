#' @importFrom R6 R6Class
NULL

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
#' tasks <- Tasks$new(tasklist=list(dummy1=dummy1, dummy2=dummy2))
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
Task <- R6::R6Class(
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


#' Create new task 
#'
#' This function generates new Tasks object.
#'
#' @section Usage:
#' @name tasks
task <- function(name, FUN, progress_patterns) {
    if (missing(name)) {stop('name argument is not optional')}
    if (missing(FUN)) {stop('FUN argument is not optional')}

}


#' @keywords internal
tmpsrc <- function(fnlist) {
    tmp <- tempfile(pattern='rworker')
    for (i in seq_along(fnlist)) {
        fname <- names(fnlist[i])
        fun <- capture.output(fnlist[[i]])
        fun <- fun[!grepl('<bytecode|environment.*>', fun)]
        fun[1] <- paste(fname, "<-", fun[1])
        write(fun, tmp, append=TRUE)
    }
    return(tmp)
}
