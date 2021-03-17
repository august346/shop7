from http import HTTPStatus

from flask import abort, jsonify, Response

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
    fmt, data = report.get(_id)

    if fmt == report.Fmt.xlsx.value:
        if not data:
            abort(HTTPStatus.NOT_ACCEPTABLE, f'Not ready')
        return Response(
            data.stream(32 * 1024),
            headers=dict(data.headers),
            direct_passthrough=True
        )
    elif fmt == report.Fmt.json.value:
        return jsonify(data)

    abort(HTTPStatus.BAD_REQUEST, f'Unknown fmt: `{fmt}`')


@app.route('/reports/', methods=['POST'])
def create_report():
    data = report.create()
    tasks.process.delay(data)
    return jsonify(data['_id'])
