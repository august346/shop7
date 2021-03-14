from functools import wraps
from typing import Callable

from celery import shared_task

from tasks.runner import get_runner


def mongo_init_before(f: Callable):
    @wraps(f)
    def wrapper(*args, **kwargs):
        from app import app
        import mongo
        mongo.client.init_app(app)

        return f(*args, **kwargs)

    return wrapper


@shared_task(name="app.process")
@mongo_init_before
def process(info: dict):
    get_runner(info).run()
