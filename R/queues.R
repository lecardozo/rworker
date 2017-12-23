#' @include redis_queue.R

QUEUES = list(
    "redis"=RedisQueue
)

queue = function(url, name) {
    params = parse_url(url)
    provider = params$provider
    params$provider = NULL
    params$name = name
    return(
        do.call(QUEUES[[provider]]$new, params)
    )
}
