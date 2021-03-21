from functools import wraps
from typing import Callable, Dict, Tuple, Type

from celery import shared_task

from . import test, wb
from .base import ETL


ETL_BUILDERS: Dict[Tuple[str, str], Type[ETL]] = {
    ('test', 'fin_month'): test.TestETL,
    ('wb', 'fin_month'): wb.WbFinMonthETL
}


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
def process(_id: str, platform: str, doc_type: str):
    ETL_BUILDERS[(platform, doc_type)](_id).run()
