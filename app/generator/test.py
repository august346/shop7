from typing import List, Dict, Any, Iterable, Tuple

import pandas as pd

from .base import Extractor, Transformer, Loader, ETL


class TestExtractor(Extractor):
    def get_rows(self) -> List[dict]:
        return list(map(
            lambda x: {'id': x, 'data': f'test_#{x}'},
            range(10)
        ))


class TestTransformer(Transformer):
    def get_transforms(self) -> Dict[str, Any]:
        transforms: Dict[str, Any] = {}

        for ind, row in enumerate(self._rows):
            transforms[f'rows.{ind}.name'] = f'name_#{row["id"]}'
            transforms[f'rows.{ind}.not_name'] = f'not_name_#{row["id"]}'

        return transforms


class TestLoader(Loader):
    def get_dataframes(self) -> Iterable[Tuple[str, pd.DataFrame]]:
        yield 'test', pd.DataFrame(self._rows)


class TestETL(ETL):
    _extractor_builder = TestExtractor
    _transformer_builder = TestTransformer
    _loader_builder = TestLoader
