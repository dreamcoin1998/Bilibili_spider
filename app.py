# -*- coding:UTF-8 -*8

from celery import Celery

app = Celery('spider', include=['spider_bili'])
app.config_from_object('config')