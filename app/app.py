from flask import jsonify

import report
import utils
from tasks import tasks

app = utils.create_app(__name__)
celery = utils.make_celery(app)


@app.route('/')
def index():
    return 'Hello world'


@app.route('/reports/<string:_id>/', methods=['GET'])
def read_report(_id: str):
    return jsonify(report.get(_id))


@app.route('/reports/', methods=['POST'])
def create_report():
    data = report.create()
    tasks.process.delay(data)
    return jsonify(data['_id'])
