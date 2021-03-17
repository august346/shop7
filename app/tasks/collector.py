import os
import time
from datetime import datetime
from functools import lru_cache, partial, wraps
from http import HTTPStatus
from typing import List, Dict, Any, Iterable, Union, Callable, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests import Response


def paused(f: Callable = None, seconds: float = 1):
    if not f:
        return partial(paused, seconds=seconds)

    @wraps(f)
    def wrapper(*args, **kwargs):
        await_time = time.time()

        if wrapper.previous_timestamp:
            await_time = wrapper.previous_timestamp + seconds

        while await_time > time.time():
            time.sleep(0.1)

        result = f(*args, **kwargs)
        wrapper.previous_timestamp = time.time()

        return result

    wrapper.previous_timestamp = None

    return wrapper


class Collector:
    report: dict

    def __init__(self, report: dict):
        self.report = report

    def get_rows(self) -> List[dict]:
        raise NotImplementedError

    def get_row_updates(self, row: dict) -> Dict[str, Any]:
        raise NotImplementedError

    def get_dataframes(self, rows: List[dict]) -> Iterable[Tuple[str, pd.DataFrame]]:
        raise NotImplementedError


class TestCollector(Collector):

    def get_rows(self) -> List[dict]:
        return [{
            'id': i,
            'data': f'test_#{i}'
        } for i in range(10)]

    def get_row_updates(self, row: dict) -> Dict[str, Any]:
        return {
            'name': f'name_#{row["id"]}',
            'not_name': f'not_name_#{row["id"]}',
        }

    def get_dataframes(self, rows: List[dict]) -> Iterable[Tuple[str, pd.DataFrame]]:
        yield 'test', pd.DataFrame(rows)


class WbFinDoc(Collector):
    api_key: str = os.environ['WB_API_KEY']
    url: str = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/reportDetailByPeriod'
    sleep_between: int = 1
    common_keys = ('nm_id', 'barcode', 'sa_name')
    unique_keys = (
        'realizationreport_id', 'order_dt', 'sale_dt', 'supplier_reward', 'supplier_oper_name', 'quantity',
        'delivery_rub'
    )

    def get_rows(self) -> List[dict]:
        return self._get_aggregated()

    def _get_aggregated(self) -> List[dict]:
        result: Dict[str, dict] = {}

        for data in self._get_payloads():
            nm_id: str = data['nm_id']

            if nm_id not in result:
                result[nm_id] = self._get_common_fields(data)
                result[nm_id]['reports'] = []

            result[nm_id]['reports'].append(self._get_unique_fields(data))

        return list(result.values())

    def _get_common_fields(self, data: dict) -> Dict[str, Union[str, int, float]]:
        return {k: data[k] for k in self.common_keys}

    def _get_unique_fields(self, data: dict):
        return {k: data[k] for k in self.unique_keys}

    def _get_payloads(self) -> Iterable[Dict[str, Union[str, int, float]]]:
        _id = 0

        while _id is not None:
            rsp: Response = self._do_request(_id)
            assert rsp.status_code == HTTPStatus.OK, (rsp, rsp.content.decode(), rsp.request.url)

            json = rsp.json()

            if not json:
                return

            yield from json

            _id = max([p['rrd_id'] for p in json])

            time.sleep(self.sleep_between)

    def _do_request(self, _id) -> Response:
        return requests.get(
            self.url,
            params=dict(
                key=self.api_key,
                limit=1000,
                rrdid=_id,
                dateFrom=datetime.fromisoformat(self.report['date_from']).isoformat(),
                dateTo=datetime.fromisoformat(self.report['date_to']).isoformat()
            )
        )

    def get_row_updates(self, row: dict) -> Dict[str, Any]:
        return {
            'name': self._get_name(row['nm_id'])
        }

    @staticmethod
    @lru_cache(maxsize=5000)
    @paused(seconds=1)
    def _get_name(nm_id: str) -> 'str':
        tag = WbFinDoc._get_soup(nm_id).find(
            'span',
            {'class': 'name'}
        )
        if tag:
            return text.strip() if (text := tag.text) else text
        raise ValueError('No span_class_name in response!')

    @staticmethod
    def _get_soup(nm_id: str) -> BeautifulSoup:
        rsp = requests.get(
            f'https://www.wildberries.ru/catalog/{nm_id}/detail.aspx',
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) '
                              'Chrome/39.0.2171.95 Safari/537.36 '
            },
        )

        if rsp.status_code != 200:
            raise ResourceWarning(f'Invalid {rsp.request.url=}, {rsp.status_code=} with {rsp.content=}')

        return BeautifulSoup(rsp.content.decode('utf-8'), 'html.parser')

    def get_dataframes(self, rows: List[dict]) -> Iterable[Tuple[str, pd.DataFrame]]:
        df: pd.DataFrame = pd.DataFrame(self._get_unpacked_rows(rows))

        group_fields = ['nm_id', 'barcode', 'sa_name'] + (['name'] if 'name' in df.columns else [])

        total = df.groupby(group_fields).apply(self.get_apply)

        yield 'sum', total.sum()
        yield 'total', total

        for _id in df.realizationreport_id.unique():
            yield f'report_{_id}', df.groupby(group_fields).apply(self.get_apply)

    @staticmethod
    def _get_unpacked_rows(rows: List[dict]) -> Iterable[dict]:
        for row in rows:
            for rep in row['reports']:
                yield {**{key: value for key, value in row.items() if key != 'reports'}, **rep}

    @staticmethod
    def get_apply(x: pd.DataFrame):
        return pd.Series(dict(
            delivery=(x.delivery_rub.where(x.supplier_oper_name == 'Логистика')).sum(),
            income=(x.supplier_reward.where(x.supplier_oper_name == 'Продажа')).sum(),
            income_number=(x.quantity.where(x.supplier_oper_name == 'Продажа')).sum(),
            refund=(x.supplier_reward.where(x.supplier_oper_name == 'Возврат')).sum(),
            refund_number=(x.quantity.where(x.supplier_oper_name == 'Возврат')).sum(),
        ))
