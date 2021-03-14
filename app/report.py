from copy import copy
from datetime import datetime, date
from typing import Dict, Any

from bson import ObjectId
from flask import request

import mongo

CREATE_FIELDS = ('platform', 'doc_type', 'date_from', 'date_to')


def get(_id: str):
    doc: dict = mongo.client.db.reports.find_one({'_id': ObjectId(_id)})
    doc['_id'] = str(doc['_id'])
    doc['date_from'] = date.fromisoformat(doc['date_from'].isoformat()[:10]).isoformat()
    doc['date_to'] = date.fromisoformat(doc['date_to'].isoformat()[:10]).isoformat()

    if 'file' in doc:
        del(doc['file'])

    return doc


def create():
    fields = {
        key: request.form.get(key)
        for key in CREATE_FIELDS
    }
    fields['state'] = 'init'
    fields['files'] = {key: file.filename for key, file in request.files.items()}
    result = {'_id': None, **fields}

    valid_fields = valid_create_fields(fields)

    result['_id'] = str(mongo.client.db.reports.insert_one(valid_fields).inserted_id)

    return result


def valid_create_fields(fields: Dict[str, Any]):
    valid = copy(fields)
    valid['date_from'] = datetime.fromisoformat(valid['date_from'])
    valid['date_to'] = datetime.fromisoformat(valid['date_to'])

    return valid

