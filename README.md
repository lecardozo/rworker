
# rworker
Long running (and remote) tasks execution in R mediated by Redis.
## Motivation
The main motivation of this package was the need for executing long running R functions
triggered by [Celery](https://github.com/celery/celery) (asynchronous task queue package for Python).

![Data flow](https://raw.githubusercontent.com/lecardozo/rworker/master/img/rworker.png)
## Usage
##### Start consuming tasks from R
```R
library(rworker)

long_running_task <- function() { Sys.sleep(10) }

# This object stores the functions you want to trigger remotelly
tasks <- Tasks$new(tasklist=list(long_running_task=long_running_task))

# This represents the queue where your tasks calls will be stored
queue <- Queue$new(provider='redis', host='localhost', port=6379, name='celery')

# This is the supervisor that spawns tasks sent to the queue
consumer <- Consumer$new(queue=queue, tasks=tasks, 
                         nworkers=2, backend_url='redis://localhost:6379')
consumer$consume()
```
The `consumer` listens to changes on the queue and spawns new processes 
(using the [**processx**](https://github.com/r-lib/processx) package)

##### Send tasks from Python
```python
from celery import Celery

worker = Celery('app', broker="redis://localhost:6379/0", backend="redis://localhost:6379/0")
worker.send_task('long_running_task')
```
