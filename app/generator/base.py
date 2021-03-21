import io
from datetime import datetime
from functools import wraps
from logging import Logger
from typing import List, Type, Iterable, Tuple, Callable, Dict, Any, Optional, Union

import pandas as pd
from bson import ObjectId

import mongo
import storage
from .utils import translate
from utils import States


logger = Logger(__name__)


class ETLStage:
    state_after: str


class NeedRows:
    _rows = Iterable[dict]

    def __init__(self, rows: List[dict]):
        self._rows = rows


class Extractor(ETLStage):
    state_after = States.extracted

    _date_from: datetime
    _date_to: datetime

    def __init__(self, date_from: datetime, date_to: datetime):
        self._date_from = date_from
        self._date_to = date_to

    def get_rows(self) -> List[dict]:
        raise NotImplementedError


class Transformer(ETLStage, NeedRows):
    state_after = States.transformed

    def get_transforms(self) -> Dict[str, Any]:
        raise NotImplementedError


class Loader(ETLStage, NeedRows):
    _costs_file_id: Optional[str]

    state_after = States.loaded

    def __init__(self, rows: List[dict], costs_file_id: Optional[str]):
        super().__init__(rows)
        self._costs_file_id = costs_file_id

    def get_dataframes(self) -> Iterable[Tuple[str, pd.DataFrame]]:
        raise NotImplementedError


def _check_rows_before(f: Callable[['ETL'], None]) -> Callable[['ETL'], None]:
    @wraps(f)
    def wrapper(self: 'ETL') -> None:
        if self._rows is None:
            self._rows = self._get_document()['rows']

        f(self)

    return wrapper


class ETL(ETLStage):
    _extractor_builder: Type[Extractor]
    _transformer_builder: Type[Transformer]
    _loader_builder: Type[Loader]

    _id: str
    _state_now: str

    _rows: List[dict] = None
    _document: Dict[str, Any] = None

    def __init__(self, _id: str):
        self._id = _id

        self._state_now = self._get_document()['state']

    def _get_document(self, fresh=False) -> Dict[str, Any]:
        if fresh or self._document is None:
            self._document = mongo.client.db.reports.find_one({'_id': ObjectId(self._id)})

        return self._document

    def _update_document(self, new_state: str,  **data) -> None:
        assert mongo.client.db.reports.update_one(
            {
                '_id': ObjectId(self._id),
                'state': self._state_now
            },
            {'$set': {'state': new_state, **data}}
        )
        self._state_now = new_state
        logger.warning(f'{new_state}: {self._id}')

    def run(self):
        while self._state_now != States.loaded:
            self._run_next_stage()

        self._update_document(States.complete)

    def _run_next_stage(self) -> None:
        {
            States.init: self._extract,
            States.extracted: self._transform,
            States.transformed: self._load,
        }[self._state_now]()

    def _extract(self):
        document = self._get_document()
        date_from, date_to = map(
            lambda date_key: document[date_key].isoformat(),
            ('date_from', 'date_to')
        )

        extractor = self._extractor_builder(date_from, date_to)
        self._rows = extractor.get_rows()

        self._update_document(extractor.state_after, rows=self._rows)

    @_check_rows_before
    def _transform(self):
        self._update_document(
            self._transformer_builder.state_after,
            **self._transformer_builder(self._rows).get_transforms()
        )

    @_check_rows_before
    def _load(self):
        loader = self._loader_builder(
            rows=self._rows,
            costs_file_id=self._get_document().get('files', {}).get('costs')
        )

        output: io.BytesIO = io.BytesIO()

        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            for sheet_name, df in loader.get_dataframes():  # type: str, pd.DataFrame
                self._translated(df).to_excel(writer, sheet_name)
            writer.save()

        output.seek(0, 0)
        storage.save(storage.Bucket.reports, self._id, output)

        self._update_document(loader.state_after)

    @staticmethod
    def _translated(df: Union[pd.DataFrame, pd.Series]):
        is_df = isinstance(df, pd.DataFrame)

        to_rename = {
            key_name: translate(key_name)
            for key_name in (df.columns if is_df else df.index)
        }

        if to_rename:
            return df.rename(to_rename, axis=[0, 1][is_df])

        return df
