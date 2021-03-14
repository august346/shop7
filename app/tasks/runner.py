from logging import Logger
from typing import List, Dict, Type

from bson import ObjectId

from .collector import Collector, TestCollector, WbFinDoc
import mongo

__all__ = ['get_runner']

logger = Logger(__name__)


def get_runner(report: dict) -> 'Runner':
    return Runner(platforms[report['platform']][report['doc_type']](report))


class Runner:
    collector: Collector
    rows: List[dict]

    def __init__(self, collector: Collector):
        self.collector = collector
        self._filter = {'_id': ObjectId(collector.report['_id'])}

    def run(self):
        mongo.client.db.reports.update_one(
            self._filter,
            {'$set': {'state': 'process'}}
        )
        logger.warning(f'process: {self._filter["_id"]}')

        mongo.client.db.reports.update_one(
            self._filter,
            {'$set': self.get_rows()}
        )
        logger.warning(f'collected: {self._filter["_id"]}')

        mongo.client.db.reports.update_one(
            self._filter,
            {'$set': self.get_updates()}
        )
        logger.warning(f'updated: {self._filter["_id"]}')

    def get_rows(self) -> dict:
        self.rows = self.collector.get_rows()

        return {
            'rows': self.rows,
            'state': 'collected'
        }

    def get_updates(self) -> dict:
        updates = {'state': 'updated'}

        for ind, row in enumerate(self.rows):
            for name, value in self.collector.get_row_updates(row).items():
                updates[f'rows.{ind}.{name}'] = value

        return updates


platforms: Dict[str, Dict[str, Type[Collector]]] = {
    'test': {
        'fin_month': TestCollector
    },
    'wb': {
        'fin_month': WbFinDoc
    }
}