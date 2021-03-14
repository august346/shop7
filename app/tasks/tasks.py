from celery import shared_task

from tasks.runner import get_runner


@shared_task(name="app.process")
def process(info: dict):
    from app import app
    import mongo

    mongo.client.init_app(app)
    get_runner(info).run()
