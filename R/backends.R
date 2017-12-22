#' @include redis_backend.R

BACKENDS = list(
    "redis"=RedisBackend
)

backend = function(url) {
    params = parse_url(url)
    provider = params$provider
    params$provider = NULL
    return(
        do.call(BACKENDS[[provider]]$new, params)
    )
}
