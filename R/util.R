#' @importFrom stringr str_match
#' @keywords internal
parse_url = function(url) {
    parsed = as.list(stringr::str_match(url, "(.*)://(.*):(\\d*)"))
    parsed = parsed[-1]
    names(parsed) <- c("provider", "host", "port")
    return(parsed)
}
