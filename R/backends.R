#' @include redis_backend.R

BACKENDS = list(
    "redis"=RedisBackend
)

backend = function(url) {
    params = parse_url(url)
    provider = params$provider
    params$provider = NULL
    params <- params[!is.na(params)]
    return(
        do.call(BACKENDS[[provider]]$new, params)
    )
}
