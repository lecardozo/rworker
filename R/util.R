#' @importFrom stringr str_match
#' @keywords internal
parse_url = function(url) {
    parsed = as.list(stringr::str_match(url, "(.*)://(.*):(\\d*)"))
    parsed = parsed[-1]
    names(parsed) <- c("provider", "host", "port")
    return(parsed)
}

#' @keywords internal
# detectCores from Parallel package
ncores = function(all.tests=FALSE, logical=TRUE){
	systems <- list(linux = if (logical) "grep processor /proc/cpuinfo 2>/dev/null | wc -l" else "cat /proc/cpuinfo | grep 'cpu cores'| uniq | cut -f2 -d:", 
                    darwin = if (logical) "/usr/sbin/sysctl -n hw.logicalcpu 2>/dev/null" else "/usr/sbin/sysctl -n hw.physicalcpu 2>/dev/null", 
                    solaris = if (logical) "/usr/sbin/psrinfo -v | grep 'Status of.*processor' | wc -l" else "/bin/kstat -p -m cpu_info | grep :core_id | cut -f2 | uniq | wc -l", 
                    freebsd = "/sbin/sysctl -n hw.ncpu 2>/dev/null", openbsd = "/sbin/sysctl -n hw.ncpu 2>/dev/null", 
                    irix = c("hinv | grep Processors | sed 's: .*::'", "hinv | grep '^Processor '| wc -l"))
    for (i in seq(systems)) if (all.tests || length(grep(paste0("^", names(systems)[i]), R.version$os))) 
    	for (cmd in systems[i]) {
        	if (is.null(a <- tryCatch(suppressWarnings(system(cmd, TRUE)), error = function(e) NULL))) 
                next
            a <- gsub("^ +", "", a[1])
            if (grepl("^[1-9]", a)) 
                return(as.integer(a))
        }
}
