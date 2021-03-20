import hashlib
from copy import copy
from datetime import datetime, date
from enum import Enum
from typing import Dict, Any, Tuple

from bson import ObjectId
from flask import request

import mongo
import storage
from utils import State

CREATE_FIELDS = ('platform', 'doc_type', 'date_from', 'date_to')


class Fmt(Enum):
    json = 'json'
    xlsx = 'xlsx'


def get(_id: str) -> Tuple[str, Any]:
    fmt = request.args.get('fmt') or 'json'

    if fmt == Fmt.json.value:
        return fmt, _get_json(_id)
    elif fmt == Fmt.xlsx.value:
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


def create():
    fields = {
        key: request.form.get(key)
        for key in CREATE_FIELDS
    }
    fields['state'] = State.init.value
    fields['files'] = get_files()
    result = {'_id': None, **fields}

    valid_fields = valid_create_fields(fields)

    result['_id'] = str(mongo.client.db.reports.insert_one(valid_fields).inserted_id)

    return result


def valid_create_fields(fields: Dict[str, Any]):
    valid = copy(fields)
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
