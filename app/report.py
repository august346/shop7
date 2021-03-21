import hashlib
from datetime import datetime, date
from typing import Dict, Any, Tuple

from bson import ObjectId
from flask import request

import mongo
import storage
from utils import States, Fmts

NECESSARY_CREATE_FIELDS = ('platform', 'doc_type', 'date_from', 'date_to')


def get(_id: str) -> Tuple[str, Any]:
    fmt = request.args.get('fmt') or 'json'

    if fmt == Fmts.json:
        return fmt, _get_json(_id)
    elif fmt == Fmts.xlsx:
        return fmt, _get_file(_id)

    return fmt, None


def _get_json(_id: str):
    doc: dict = mongo.client.db.reports.find_one({'_id': ObjectId(_id)})
    doc['_id'] = str(doc['_id'])
    doc['date_from'] = date.fromisoformat(doc['date_from'].isoformat()[:10]).isoformat()
    doc['date_to'] = date.fromisoformat(doc['date_to'].isoformat()[:10]).isoformat()

    if 'file' in doc:
        del (doc['file'])

    return doc


def _get_file(_id: str):
    return storage.get(storage.Bucket.reports, _id)


def create() -> Tuple[str, str, str]:
    result = {
        'state': States.init,
        'files': get_files(),
        **valid_create_fields(),
    }

    oid = str(mongo.client.db.reports.insert_one(result).inserted_id)

    return str(oid), result['platform'], result['doc_type']


def valid_create_fields():
    valid = {
        key: request.form[key]
        for key in NECESSARY_CREATE_FIELDS
    }
    valid['date_from'] = datetime.fromisoformat(valid['date_from'])
    valid['date_to'] = datetime.fromisoformat(valid['date_to'])

    return valid


def get_files() -> Dict[str, str]:
    result = {}

    for key, file in request.files.items():
        name = hashlib.md5(file.read()).hexdigest()
        file.seek(0)

        storage.save(
            bucket=storage.Bucket.files,
            name=name,
            file=file
        )
        result[key] = name

    return result
