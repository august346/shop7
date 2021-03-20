import os

MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/shop7?authSource=admin')
CELERY_BROKER_URL = os.environ.get('RABBITMQ_URL', 'amqp://localhost:5672/')
CELERY_RESULT_BACKEND = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
