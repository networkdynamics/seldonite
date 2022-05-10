from email import header
import pyspark.sql.functions as sfuncs
import pyspark.sql as psql
import pyspark.ml as pml
from sparknlp.pretrained import PretrainedPipeline

from seldonite import base, graphs

def accumulate_embeddings(df, embeddings_df, len_embed, token_col='token', embedding_col='token_embedding'):
    df = df.join(embeddings_df, token_col)
    df = df.groupBy('id').agg(sfuncs.array(*[sfuncs.sum(sfuncs.col(embedding_col)[i]) for i in range(len_embed)]).alias('embedding'))
    df = df.drop(token_col, embedding_col)

    return df

class Embed(base.BaseStage):
    def __init__(self, input):
        super().__init__(input)
        self._do_news2vec_embed = False

    def news2vec_embed(self, embedding_path):
        self._do_news2vec_embed = True
        self._news2vec_embedding_path = embedding_path
        return self

    def _news2vec_embed(self, df, spark_manager):
        
        Z1 = 1
        Z2 = 2
        TOP_NUM_NODE = 20

        df.cache()

        nodes_df = graphs.get_nodes_df(df)
        df.unpersist()

        # increase number of partitions because of new columns
        num_partitions = nodes_df.rdd.getNumPartitions()
        nodes_df = nodes_df.repartition(num_partitions * 8)

        nodes_df.cache()

        # explode tfidf again to get edges
        article_nodes_df = nodes_df.where(sfuncs.col('text_top_n').isNotNull() & sfuncs.col('title_top_n').isNotNull())

        edges_df = graphs.get_edges_df(article_nodes_df)
        edges_df.cache()

        # divide each weight by scalar
        edges_df = edges_df.select('id1', 'word', (sfuncs.col('title_value') / Z1).alias('title_value'), (sfuncs.col('text_value') / Z2).alias('text_value'))

        # add up values
        word_edges_df = edges_df.select('id1', 'word', (sfuncs.coalesce('title_value', sfuncs.lit(0)) + sfuncs.coalesce('text_value', sfuncs.lit(0))).alias('weight'))
        
        # concat all top tfidf words
        w = psql.Window.partitionBy('id').orderBy(sfuncs.desc('weight'))
        article_tokens_df = word_edges_df.withColumnRenamed('id1', 'id') \
                                         .withColumn('rank',sfuncs.row_number().over(w)) \
                                         .where(sfuncs.col('rank') <= TOP_NUM_NODE) \
                                         .select('id', sfuncs.col('word').alias('token'))

        # get article length
        article_nodes_df = article_nodes_df.withColumn('num_words', sfuncs.size('title_tokens') + sfuncs.size('text_tokens'))
        article_nodes_df = article_nodes_df.withColumn('num_words_ord', sfuncs.when((sfuncs.col('num_words') >= 0) & (sfuncs.col('num_words')  <= 200), 'wc200') \
                                                                              .when((sfuncs.col('num_words') > 200) & (sfuncs.col('num_words') <= 500), 'wc500') \
                                                                              .when((sfuncs.col('num_words') > 500) & (sfuncs.col('num_words') <= 1000), 'wc1000') \
                                                                              .when((sfuncs.col('num_words') > 1000) & (sfuncs.col('num_words') <= 2000), 'wc2000') \
                                                                              .when((sfuncs.col('num_words') > 2000) & (sfuncs.col('num_words') <= 3000), 'wc3000') \
                                                                              .when((sfuncs.col('num_words') > 3000) & (sfuncs.col('num_words') <= 5000), 'wc5000') \
                                                                              .when(sfuncs.col('num_words') > 5000, 'wcmax'))
        article_nodes_df = article_nodes_df.drop('num_words')
        article_tokens_df = article_tokens_df.union(article_nodes_df.select('id', sfuncs.col('num_words_ord').alias('token')))

        # get article sentiment
        sentiment_pipeline = PretrainedPipeline("classifierdl_bertwiki_finance_sentiment_pipeline", lang = "en")
        article_nodes_df = sentiment_pipeline.annotate(article_nodes_df, 'text') \
                                             .select('*', sfuncs.col('class.result').getItem(0).alias('sentiment')) \
                                             .drop('document', 'sentence_embeddings', 'class')
        article_tokens_df = article_tokens_df.union(article_nodes_df.select('id', sfuncs.col('sentiment').alias('token')))

        # get month 
        article_nodes_df = article_nodes_df.withColumn('month', sfuncs.concat(sfuncs.lit('m_'), sfuncs.month('publish_date')))
        article_tokens_df = article_tokens_df.union(article_nodes_df.select('id', sfuncs.col('month').alias('token')))
        # get day of month
        article_nodes_df = article_nodes_df.withColumn('day_of_month', sfuncs.concat(sfuncs.lit('d_'), sfuncs.dayofmonth('publish_date')))
        article_tokens_df = article_tokens_df.union(article_nodes_df.select('id', sfuncs.col('day_of_month').alias('token')))
        # get of week
        article_nodes_df = article_nodes_df.withColumn('day_of_week', sfuncs.concat(sfuncs.lit('wd_'), sfuncs.dayofweek('publish_date')))
        article_tokens_df = article_tokens_df.union(article_nodes_df.select('id', sfuncs.col('day_of_week').alias('token')))

        # load embeddings from file
        spark = spark_manager.get_spark_session()
        embeddings_df = spark.read.csv(self._news2vec_embedding_path, header=True)

        # sort out columns
        embeddings_df = embeddings_df.withColumnRenamed('_c0', 'token')

        embed_col_names = embeddings_df.columns
        embed_col_names.remove('token')
        embeddings_df = embeddings_df.withColumn('token_embedding', sfuncs.array([sfuncs.col(col_name) for col_name in embed_col_names]))
        embeddings_df = embeddings_df.drop(*embed_col_names)

        article_embeds_df = accumulate_embeddings(article_tokens_df, embeddings_df, len(embed_col_names))

        article_df = article_df.join(article_embeds_df, 'id')
        article_df = article_df.select('title', 'text', 'publish_date', 'url', 'embedding')
        return article_df


    def _process(self, spark_manager):
        res = self.input._process(spark_manager)

        if self._do_news2vec_embed:
            return self._news2vec_embed(res, spark_manager)
