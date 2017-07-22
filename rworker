#!/usr/bin/Rscript

"rworker - R consumer for Celery + Redis  
Usage: rworker.R TASKFILE [--queue=<QUEUE> --redis-addr=<ADDR> --redis-port=<PORT> --workers=<WORKERS> --verbose]

Input:
  TASKFILE                         file containing your tasks (functions)
Options:
  -h --help                         show this help message
  --version                         show program version
  -q --queue=<ADDR>                 Queue name [default: queue]
  -r --redis-addr=<ADDR>            Redis instance address [default: redis]
  -p --redis-port=<PORT>            Redis port [default: 6379]
  -w --workers=<WORKERS>            number of worker processes [default: 2]
  -v --verbose                      display task execution
Author:
  Lucas E Cardozo - lucas.cardozo at usp.br
" -> doc

suppressMessages({
    library(docopt) 
    library(rredis)
    library(jsonlite)
    library(base64enc)
    library(processx)
})

arg <- docopt(doc, version="0.0.1\n", strict=TRUE)
arg <- arg[!sapply(arg, is.null)][-(1:2)]  # filter missing, 'help' and 'version'
clean <- function(s) gsub('-', '_', gsub('^-+', '', tolower(s)))
names(arg) <- clean(names(arg))
parameters <- arg

redisConnect(host=parameters[['redis_addr']], port=as.numeric(parameters[['redis_port']]))
workerlist <- list()

cat(sprintf('Listening to Redis in %s ...\n', parameters[['redis_addr']]))

while (TRUE) {
    msg <- redisLPop(parameters[['queue']])
    # adds new jobs
    if (!is.null(msg)){
        if (length(workerlist) < as.numeric(parameters[['workers']])) {
            action <- fromJSON(msg)
            task <- action$headers$task
            task_id <- action$headers$id
            cat('Received task', task_id, ':', task, '\n')
            params <- fromJSON(rawToChar(base64_dec(action$body)))[[2]]
            params <- paste(paste0(names(params),'=',params), collapse=',')
            args <- sprintf('source("%s"); %s(%s)', parameters[['taskfile']], task, params)
            workerlist[[task_id]] <- process$new(command='/usr/bin/Rscript',
                                                 args=c('-e', args),
                                                 stdout='|', stderr='|',
                                                 echo_cmd=TRUE)
        } else {
            # push task back into the queue
            redisLPush(parameters[['queue']], msg)
        }
    }
    
    # check process status
    if (length(workerlist) > 0 ){
        to_remove <- sapply(workerlist, function(proc){
            if (!proc$is_alive()) {
                return(FALSE)
            } else {
                if (proc$is_incomplete_output()){
                    out <- proc$read_output_lines()
                    if (length(out) != 0 && parameters[['verbose']]) {
                        sapply(out, function(x) cat('INFO: ',x,'\n'))
                    }
                }
                if (proc$is_incomplete_error()){
                    err <- proc$read_error_lines()
                    if (length(err) != 0 && parameters[['verbose']]) {
                        cat('ERROR: ',err,'\n')
                    }
                }
                return(TRUE)
            }
        }, simplify=TRUE)
        workerlist <- workerlist[to_remove]
    }
    Sys.sleep(2)
}
