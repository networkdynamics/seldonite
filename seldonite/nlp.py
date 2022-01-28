import re
import subprocess

import nltk
import pyspark.sql as psql
import pyspark.ml as sparkml
import sparknlp

from seldonite import collect

class NLP:
    collector: collect.Collector

    def __init__(self, collector):
        self.collector = collector

        self.do_tfidf = False

    def top_tfidf(self, top_num):
        self.do_tfidf = True
        self.tfidf_top_num = top_num

    def _set_spark_options(self, spark_builder):
        spark_builder.add_package('com.johnsnowlabs.nlp:spark-nlp_2.12:3.4.0')

        self.collector._set_spark_options(spark_builder)
    
    def _process(self, spark_manager):
        df = self.collector._fetch(spark_manager)
        
        eng_stopwords = nltk.corpus.stopwords.words('english')

        documentAssembler = sparknlp.base.DocumentAssembler() \
            .setInputCol('consumer_complaint_narrative') \
            .setOutputCol('document')
            
        tokenizer = sparknlp.annotator.Tokenizer() \
            .setInputCols(['document']) \
            .setOutputCol('token')
            
        # note normalizer defaults to changing all words to lowercase.
        # Use .setLowercase(False) to maintain input case.
        normalizer = sparknlp.annotator.Normalizer() \
            .setInputCols(['token']) \
            .setOutputCol('normalized') \
            .setLowercase(True)
            
        # note that lemmatizer needs a dictionary. So I used the pre-trained
        # model (note that it defaults to english)
        lemmatizer = sparknlp.annotator.LemmatizerModel.pretrained() \
            .setInputCols(['normalized']) \
            .setOutputCol('lemma')
            
        stopwords_cleaner = sparknlp.annotator.StopWordsCleaner() \
            .setInputCols(['lemma']) \
            .setOutputCol('clean_lemma') \
            .setCaseSensitive(False) \
            .setStopWords(eng_stopwords)# finisher converts tokens to human-readable output

        finisher = sparknlp.base.Finisher() \
            .setInputCols(['clean_lemma']) \
            .setCleanAnnotations(False)

        pipeline = sparkml.Pipeline() \
            .setStages([
                documentAssembler,
                tokenizer,
                normalizer,
                lemmatizer,
                stopwords_cleaner,
                finisher
            ])

        # tokenize, lemmatize, remove stop words
        df = pipeline.fit(df) \
                     .transform(df)

        # get term frequency
        cv = sparkml.feature.CountVectorizer(inputCol="finish_clean_lemma", outputCol="features")
        cv_model = cv.fit(df)
        df = cv_model.transform(df)

        # get inverse document frequency
        idf = sparkml.feature.IDF(inputCol="raw_features", outputCol="features")
        idf_model = idf.fit(df)
        df = idf_model.transform(df)

        return df

