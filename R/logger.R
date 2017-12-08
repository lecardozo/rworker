log_it = function(string, state) {
    if (state=='error') {
        futile.logger::flog.error(crayon::red(string))
    } else if (state=='info') {
        futile.logger::flog.info(crayon::blue(string))
    } else if (state=='success') {
        futile.logger::flog.info(crayon::green(string))
    }
}
