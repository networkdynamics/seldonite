import os
import zipfile

import pandas as pd
import pyspark.sql as psql

from seldonite import filters
from seldonite.helpers import utils

def test_preprocess():
    this_dir_path = os.path.dirname(os.path.abspath(__file__))
    fixture_path = os.path.join(this_dir_path, '..', 'fixtures')
    data_path = os.path.join(fixture_path, 'to_preprocess_political.csv')
    df = pd.read_csv(data_path)

    rows = (psql.Row(url=row['url'], all_text=row['all_text']) for idx, row in df.iterrows())

    # make sure the file are where we need them
    filters.political.ensure_zip_exists()
    zip_path = os. path.join(this_dir_path, '..', '..', 'seldonite', 'filters', 'pon_classifier.zip')
    utils.unzip(zip_path, fixture_path)

    tokens = utils.map_col_with_index(rows, 'url', 'all_text', 'tokens', filters.political.preprocess, tokenizer_path=fixture_path)
    tokens = list(tokens)

    # clean up files
    os.remove(os.path.join(fixture_path, 'model.h5'))
    os.remove(os.path.join(fixture_path, 'tokenizer.json'))

    assert len(tokens) == len(df)