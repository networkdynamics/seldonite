from lib2to3.pgen2 import token
import re
import subprocess

from gensim import models, corpora
import nltk
import pyspark.sql as psql
import pyspark.sql.functions as sfuncs
import pyspark.ml as sparkml
import sparknlp

from seldonite import collect

class NLP:
    collector: collect.Collector

    def __init__(self, collector):
        self.collector = collector

        self.do_tfidf = False

    def top_tfidf(self, top_num, save_path=None, load_path=None):
        self.do_tfidf = True
        self.tfidf_top_num = top_num
        self.tfidf_save_path = save_path
        self.tfidf_load_path = load_path
        return self

    def _tfidf(self, df: psql.DataFrame, spark_manager):
        try:
            eng_stopwords = nltk.corpus.stopwords.words('english')
        except LookupError as e:
            nltk.download('stopwords')
            eng_stopwords = nltk.corpus.stopwords.words('english')

        stages = []
        cols_to_drop = []

        text_cols = ['title', 'text']
        for text_col in text_cols:
            doc_out_col = f"{text_col}_document"
            document_assembler = sparknlp.base.DocumentAssembler() \
                .setInputCol(text_col) \
                .setOutputCol(doc_out_col)
                
            tokenizer_out_col = f"{text_col}_token"
            tokenizer = sparknlp.annotator.Tokenizer() \
                .setInputCols(doc_out_col) \
                .setOutputCol(tokenizer_out_col)
                
            # note normalizer defaults to changing all words to lowercase.
            # Use .setLowercase(False) to maintain input case.
            normalizer_out_col = f"{text_col}_normalized"
            normalizer = sparknlp.annotator.Normalizer() \
                .setInputCols(tokenizer_out_col) \
                .setOutputCol(normalizer_out_col) \
                .setLowercase(True)
                
            # note that lemmatizer needs a dictionary. So I used the pre-trained
            # model (note that it defaults to english)
            lemma_out_col = f"{text_col}_lemma"
            lemmatizer = sparknlp.annotator.LemmatizerModel.pretrained() \
                .setInputCols(normalizer_out_col) \
                .setOutputCol(lemma_out_col)
                
            cleaner_out_col = f"{text_col}_clean_lemma"
            stopwords_cleaner = sparknlp.annotator.StopWordsCleaner() \
                .setInputCols(lemma_out_col) \
                .setOutputCol(cleaner_out_col) \
                .setCaseSensitive(False) \
                .setStopWords(eng_stopwords)# finisher converts tokens to human-readable output

            finisher_out_col = f"{text_col}_tokens"
            finisher = sparknlp.base.Finisher() \
                .setInputCols(cleaner_out_col) \
                .setOutputCols(finisher_out_col) \
                .setCleanAnnotations(False)

            

            cols_to_drop.extend([
                doc_out_col,
                tokenizer_out_col,
                normalizer_out_col,
                lemma_out_col,
                cleaner_out_col
            ])

            stages.extend([
                document_assembler,
                tokenizer,
                normalizer,
                lemmatizer,
                stopwords_cleaner,
                finisher
            ])

        pipeline = sparkml.Pipeline() \
            .setStages(stages)

        # increase number of partitions because of new columns
        num_partitions = df.rdd.getNumPartitions()
        df = df.repartition(num_partitions * 4)

        # tokenize, lemmatize, remove stop words
        df = pipeline.fit(df) \
                     .transform(df)

        df = df.drop(*cols_to_drop)

        all_tokens_col = 'all_tokens'
        df = df.withColumn(all_tokens_col, sfuncs.concat(df['text_tokens'], df['title_tokens']))
        cv = sparkml.feature.CountVectorizer()
        cv.setInputCol(all_tokens_col)

        cv_model = cv.fit(df)
        df = df.drop(all_tokens_col)

        # create vocab lookup
        spark = spark_manager.get_spark_session()
        schema = psql.types.StructType([
            psql.types.StructField("word_idx", psql.types.IntegerType(), True),
            psql.types.StructField("word", psql.types.StringType(), True)
        ])
        vocab_df = spark.createDataFrame([(id, word) for id, word in enumerate(cv_model.vocabulary)], schema)

        # add index
        df = df.withColumn("id", sfuncs.monotonically_increasing_id())

        # udfs
        sparse_to_map_udf = sfuncs.udf(lambda vec : dict(zip(vec.indices.tolist(),vec.values.tolist())),psql.types.MapType(psql.types.StringType(),psql.types.StringType()))

        for text_col in text_cols:
            # get term frequency
            token_col = f"{text_col}_tokens"
            count_feat_col = f"{text_col}_raw_features"
            cv_model.setInputCol(token_col)
            cv_model.setOutputCol(count_feat_col)
            df = cv_model.transform(df)
            
            # get inverse document frequency
            tfidf_col = f"{text_col}_features"
            idf = sparkml.feature.IDF(inputCol=count_feat_col, outputCol=tfidf_col)
            idf_model = idf.fit(df)

            idf_model.save(self.tfidf_save_path)

            df = idf_model.transform(df)

            # flatten output features column to get indices & value
            value_df = df.select('id', sfuncs.explode(sparse_to_map_udf(df[tfidf_col])).name('word_idx','value'))

            # get top n words for each document(label) filtering based on its rank and join both DFs and collect & sort to get the words along with its value
            w = psql.Window.partitionBy('id').orderBy(sfuncs.desc('value'))
            value_df = value_df.withColumn('rank',sfuncs.row_number().over(w)) \
                                .where(sfuncs.col('rank') <= self.tfidf_top_num)
            top_word_df = value_df.join(vocab_df, 'word_idx') \
                                    .groupby('id') \
                                    .agg(sfuncs.sort_array(sfuncs.collect_list(sfuncs.struct(sfuncs.col('value'),sfuncs.col('word'))),asc=False).name(f"{text_col}_top_n"))

            df = df.join(top_word_df, 'id')
            df = df.drop(count_feat_col, tfidf_col)

        return df

    def _set_spark_options(self, spark_builder):
        spark_builder.use_spark_nlp()
        self.collector._set_spark_options(spark_builder)
    
    def _process(self, spark_manager):
        df = self.collector._process(spark_manager)
        
        if self.do_tfidf:
            df = self._tfidf(df, spark_manager)

        return df


    def find_topics(self, batch_size=1000):
        articles = self.fetch()
        prepro = self._process()

        more_articles = True
        model = None
        dictionary = None
        while more_articles:
            batch_idx = 0
            content_batch = []

            while batch_idx < batch_size:
                try:
                    article = next(articles)
                    content_batch.append(article.text)
                    batch_idx += 1
                except StopIteration:
                    more_articles = False
                    break

            # TODO add bigrams
            docs = list(prepro.preprocess(content_batch))

            if not dictionary:
                # TODO consider using hashdictionary
                dictionary = corpora.Dictionary(docs)

                no_below = max(1, batch_size // 100)
                dictionary.filter_extremes(no_below=no_below, no_above=0.9)
            
            corpus = [dictionary.doc2bow(doc) for doc in docs]

            if not model:
                # need to 'load' the dictionary
                dictionary[0]
                # TODO use ldamulticore for speed
                model = models.LdaModel(corpus, 
                                        id2word=dictionary.id2token, 
                                        num_topics=10)
            else:
                model.update(corpus)

        return model, dictionary

