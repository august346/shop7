from http import HTTPStatus

from flask import abort, jsonify, Response

from report import get, create
from generator import tasks
from utils import Fmts, create_app, make_celery

app = create_app(__name__)
celery = make_celery(app)


@app.route('/')
def index():
    return 'Hello world'


@app.route('/reports/<string:_id>/', methods=['GET'])
def read_report(_id: str):
    fmt, data = get(_id)

    if fmt == Fmts.xlsx:
        if not data:
            abort(HTTPStatus.NOT_ACCEPTABLE, f'Not ready')
        return Response(
            data.stream(32 * 1024),
            headers=dict(data.headers),
            direct_passthrough=True
        )
    elif fmt == Fmts.json:
        return jsonify(data)

    abort(HTTPStatus.BAD_REQUEST, f'Unknown fmt: `{fmt}`')


@app.route('/reports/', methods=['POST'])
def create_report():
    _id, platform, doc_type = create()
    tasks.process.delay(_id, platform, doc_type)
    return jsonify(_id)
