from celery import Celery
from flask import Flask

import mongo


class States:
    init = 'init'
    extracted = 'extracted'
    transformed = 'transformed'
    loaded = 'loaded'
    complete = 'complete'


class Fmts:
    json = 'json'
    xlsx = 'xlsx'


def make_celery(app):
    celery_app = Celery(
        app.import_name,
        backend=app.config['CELERY_RESULT_BACKEND'],
        broker=app.config['CELERY_BROKER_URL'],
    )

    class ContextTask(celery_app.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery_app.Task = ContextTask
    return celery_app


def create_app(name):
    app = Flask(name)
    app.config.from_pyfile('settings.py', silent=True)
    mongo.client.init_app(app)
    make_celery(app)
    return app
