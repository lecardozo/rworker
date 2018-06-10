
# rworker [![Travis-CI Build Status](https://travis-ci.org/lecardozo/rworker.svg?branch=master)](https://travis-ci.org/lecardozo/rworker)
Celery worker for R tasks.
## Motivation
The main motivation for this package was the need for executing long running R functions
triggered by [Celery](https://github.com/celery/celery) (asynchronous task queue package for Python).

![Data flow](https://raw.githubusercontent.com/lecardozo/rworker/master/img/rworker.png)
## Usage
##### Start consuming tasks from R
```R
library(rworker)
library(magrittr)

# Broker url
redis_url <- 'redis://localhost:6379'

# Instantiate Rworker object
consumer <- rworker(name='celery', workers=2, queue=redis_url, backend=redis_url)

# Register tasks
(function(){
    Sys.sleep(5)
}) %>% consumer$task(name='long_running_task')

# Start consuming messages
consumer$consume()
```
The `rworker` function returns a `Rworker` object. This object is responsible for registering tasks, listening for messages coming from the message queue and triggering tasks execution on background processes

##### Send tasks from Python
```python
from celery import Celery

worker = Celery('app', broker="redis://localhost:6379/0", backend="redis://localhost:6379/0")
worker.send_task('long_running_task')
```

#### Tutorial
You can find more information about usage [here](https://lecardozo.github.io/rworker).

## TODO (Not ordered y priority)
- Add support for RabbitMQ, Kafka
- Add support for different serialization methods
- Implement worker acknowledgement
