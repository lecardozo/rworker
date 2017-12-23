context('Queue providers')

interface_methods = c('push', 'pull', 'connect')
mapping = list(
    c('redis://localhost:6379', RedisQueue)
)

name = 'celery'

test_that('Queue generator return correct provider', {
    for (url_class in mapping) {
        qe = queue(url_class[[1]], name)
        expect_true(url_class[[2]]$classname %in% class(qe))
    }
})

test_that('Queue class has all interface methods', {
    lapply(mapping, function(url_class) {
        qclass = url_class[[2]]
        class_methods = names(qclass$public_methods)
        intersection = intersect(interface_methods,class_methods)
        expect_equal(length(intersection), length(interface_methods))
    })
})
