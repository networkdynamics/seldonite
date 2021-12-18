import os

import pandas as pd
import pyspark.sql as psql

from seldonite import filters
from seldonite.helpers import utils

def test_preprocess():
    this_dir_path = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(this_dir_path, '..', 'fixtures', 'to_preprocess_political.csv')
    df = pd.read_csv(data_path)

    rows = []
    for idx, row in df.iterrows():
        rows.append(psql.Row(url=row['url'], all_text=row['all_text']))

    tokens = utils.map_col_with_index(rows, 'url', 'all_text', 'tokens', filters.political.preprocess)
    tokens = [token for token in tokens]

    assert len(tokens) == len(rows)