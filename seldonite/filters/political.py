__author__ = 'Lukas Gebhard <freerunningapps@gmail.com>'

import doctest
import os

from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing.text import tokenizer_from_json
from tensorflow.keras.preprocessing import sequence
import numpy as np
import pandas as pd
import pyspark.sql as psql
from bigdl.orca.learn.tf2.estimator import Estimator

from seldonite.helpers import utils

_POLITICAL_ARTICLE = '''White House declares war against terror. The US government officially announced a ''' \
                     '''large-scale military offensive against terrorism. Today, the Senate agreed to spend an ''' \
                     '''additional 300 billion dollars on the advancement of combat drones to be used against ''' \
                     '''global terrorism. Opposition members sharply criticize the government. ''' \
                     '''"War leads to fear and suffering. ''' \
                     '''Fear and suffering is the ideal breeding ground for terrorism. So talking about a ''' \
                     '''war against terror is cynical. It's actually a war supporting terror."'''
_NONPOLITICAL_ARTICLE = '''Table tennis world cup 2025 takes place in South Korea. ''' \
                        '''The 2025 world cup in table tennis will be hosted by South Korea, ''' \
                        '''the Table Tennis World Commitee announced yesterday. ''' \
                        '''Three-time world champion, Hu Ho Han, did not pass the qualification round, ''' \
                        '''to the advantage of underdog Bob Bobby who has been playing outstanding matches ''' \
                        '''in the National Table Tennis League this year.'''

def ensure_zip_exists():
    # TODO cannot currently download straight from drive
    this_dir_path = os.path.dirname(os.path.abspath(__file__))
    if not os.path.exists(os.path.join(this_dir_path, 'pon_classifier.zip')):
        raise FileNotFoundError(f"Please ensure the political news filter classifier archive is downloaded and in '{this_dir_path}'. The archive can be downloaded from 'https://drive.google.com/drive/folders/1kmFr3WYOa7bSQELvpMcY77wH4gzLe9cJ'")

def unzip_model():
    ensure_zip_exists()
    this_dir_path = os.path.dirname(os.path.abspath(__file__))
    model_dir_path = os.path.join(this_dir_path, 'pon_classifier')
    model_path = os.path.join(model_dir_path, 'model.h5')
    if not os.path.exists(model_path):
        zip_path = os.path.join(this_dir_path, 'pon_classifier.zip')
        utils.unzip(zip_path, model_dir_path)
    return model_path

def preprocess(texts, tokenizer_path=os.path.join('.', 'pon_classifier')):
    '''
    :param df:
    '''
    PADDING_SIZE = 1500
    with open(os.path.join(tokenizer_path, 'tokenizer.json'), 'r') as tokenizer_file:
            json = tokenizer_file.read()

    tokenizer = tokenizer_from_json(json)
    # TODO check what words are missing from tokenizer
    # for now we will assume vocab is good
    tokens = tokenizer.texts_to_sequences(texts)
    return sequence.pad_sequences(tokens, maxlen=PADDING_SIZE)

def spark_predict(df, feature_col, model_path=os.path.join('.', 'pon_classifier')):
    BATCH_SIZE = 256

    model_path = model_path
    def model_creator(config):
        model_path = os.path.join(model_path, 'model.h5')
        if not os.path.exists(model_path):
            model_path = unzip_model()

        return load_model(model_path)

    est = Estimator.from_keras(model_creator=model_creator)
    return est.predict(df, feature_cols=[feature_col], batch_size=BATCH_SIZE)

def filter(news_articles, threshold=0.5):
    """
    Filter out all news articles that do not cover policy topics.

    # Arguments
        news_articles: A 1D NumPy array of news articles. A news article is the string concatenation of title,
            lead paragraph, and body.
        threshold: A value in [0, 1]. The higher the threshold, the more aggressive is the filter.
            The evaluation statistics (see `README.md`) are based on a threshold of 0.5.

    # Returns
        The filtered list of news articles.

    >>> assert _POLITICAL_ARTICLE == filter_news([_POLITICAL_ARTICLE, _NONPOLITICAL_ARTICLE])[0]
    """

    classifier = Classifier()
    estimations = classifier.estimate(news_articles)
    return [a for a, p in zip(news_articles, estimations) if p >= threshold]


class Classifier:
    """
    A machine learning classifier that estimates if an English news article covers policy topics.

    The classifier is based on Heng Zheng's convolutional neural network, published at
    <https://www.kaggle.com/hengzheng/news-category-classifier-val-acc-0-65?scriptVersionId=4623537>
    under the Apache 2.0 license <http://www.apache.org/licenses/LICENSE-2.0>.
    """

    def __init__(self):
        self._tokenizer = None
        self._model = None
        self._load()

    def _load(self):
        with open('./pon_classifier/tokenizer.json', 'r') as tokenizer_file:
            json = tokenizer_file.read()

        self._tokenizer = tokenizer_from_json(json)
        self._model = load_model('./pon_classifier/model.h5')

    @staticmethod
    def _as_array(tokens):
        return np.array(tokens.values.tolist())

    def estimate(self, news_articles):
        """
        For each given news article, estimate if it covers policy topics.

        # Arguments
            news_articles: A 1D NumPy array of news articles. A news article is the string concatenation of title,
                lead paragraph, and body.

        # Returns
            The estimated probabilities as a list of length `len(news_articles)`.

        >>> classifier = Classifier()
        >>> estimations = classifier.estimate([_POLITICAL_ARTICLE, _NONPOLITICAL_ARTICLE])
        >>> estimations[0] > 0.99
        True
        >>> estimations[1] < 0.01
        True
        """

        to_estimate = EstimationSet(data=news_articles, tokenizer=self._tokenizer).get_data()
        tokens = to_estimate[EstimationSet.COL_TOKENS]
        estimations = self._model.predict(Classifier._as_array(tokens), batch_size=256)

        return [float(p) for p in list(estimations[:, 1])]


class EstimationSet:
    COL_TOKENS = 'TOKENS'
    _COL_TEXT = 'TEXT'

    def __init__(self, data, tokenizer):
        self._data = pd.DataFrame({EstimationSet._COL_TEXT: data})
        self._tokenizer = tokenizer
        self._preprocess()

    def get_data(self):
        return self._data

    def _preprocess(self):
        self._data[EstimationSet.COL_TOKENS] = self._tokenizer.texts_to_sequences(self._data[EstimationSet._COL_TEXT])
        self._data[EstimationSet.COL_TOKENS] = EstimationSet._pad_tokens(self._data[EstimationSet.COL_TOKENS], 1500)

    @staticmethod
    def _pad_tokens(tokens, padding_size):
        return pd.Series(list(sequence.pad_sequences(tokens, maxlen=padding_size)), index=tokens.index)


if __name__ == '__main__':
    doctest.testmod(raise_on_error=True)
