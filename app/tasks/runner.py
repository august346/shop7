import io
from logging import Logger
from typing import List, Dict, Type, Optional

from bson import ObjectId
import pandas as pd

import storage
from utils import State
from .collector import Collector, TestCollector, WbFinDoc
import mongo

__all__ = ['get_runner']

logger = Logger(__name__)


def get_runner(report: dict) -> 'Runner':
    return Runner(platforms[report['platform']][report['doc_type']](report))


class Runner:
    _id: str
    _collector: Collector
    _rows: List[dict]

    def __init__(self, collector: Collector):
        self._id = collector.report['_id']
        self._collector = collector
        self._filter = {'_id': ObjectId(self._id)}

        self._upd_document(State.process)

    def run(self):
        self._collect()
        # self._update()
        self._generate()

        self._upd_document(State.complete)

    def _upd_document(self, state: State, to_set: Optional[dict] = None):
        to_set = to_set or {}
        to_set['state'] = state.value

        mongo.client.db.reports.update_one(self._filter, {'$set': to_set})
        logger.warning(f'{state}: {self._filter["_id"]}')

    def _collect(self) -> None:
        self._rows = self._collector.get_rows()
        self._upd_document(State.collected, {'rows': self._rows})

    def _update(self) -> None:
        updates = {}

        for ind, row in enumerate(self._rows):
            for name, value in self._collector.get_row_updates(row).items():
                updates[f'rows.{ind}.{name}'] = row['name'] = value

        self._upd_document(State.updated, updates)

    def _generate(self) -> None:
        file_id = self._id

        output: io.BytesIO = io.BytesIO()

        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            for sheet_name, df in self._collector.get_dataframes(self._rows):
                df.to_excel(writer, sheet_name)
            writer.save()

        output.seek(0, 0)
        storage.save(storage.Bucket.reports, file_id, output)


platforms: Dict[str, Dict[str, Type[Collector]]] = {
    'test': {
        'fin_month': TestCollector
    },
    'wb': {
        'fin_month': WbFinDoc
    }
}
