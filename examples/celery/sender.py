from celery import Celery

worker = Celery('main', broker='redis://redis:6379/0')

worker.send_task('add',args=[1,1])
