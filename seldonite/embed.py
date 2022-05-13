from email import header
import pyspark.sql.functions as sfuncs
import pyspark.sql as psql
import pyspark.ml as pml
from sparknlp.pretrained import PretrainedPipeline

from seldonite import base, graphs

def accumulate_embeddings(df, embeddings_df, feature_cols, len_embed, feature_vals, token_col='token', embedding_col='token_embedding'):
    
    all_embeds_df = None
    for feature_col in feature_cols:
        if feature_col in feature_vals:
            broadcast_embeds = sfuncs.broadcast(embeddings_df.where(sfuncs.col(token_col).isin(feature_vals[feature_col])))
        else:
            broadcast_embeds = sfuncs.broadcast(embeddings_df)
        feature_df = df.select('id', feature_col).join(broadcast_embeds, sfuncs.col(feature_col) == sfuncs.col(token_col))
        feature_df = feature_df.drop(token_col)
        if all_embeds_df:
            all_embeds_df = all_embeds_df.union(feature_df)
        else:
            all_embeds_df = feature_df

    df = all_embeds_df.groupBy('id').agg(sfuncs.array(*[sfuncs.sum(sfuncs.col(embedding_col)[i]) for i in range(len_embed)]).alias('embedding'))

    return df

class Embed(base.BaseStage):
    def __init__(self, input):
        super().__init__(input)
        self._do_news2vec_embed = False

    def news2vec_embed(self, embedding_path, export_features=False):
        self._do_news2vec_embed = True
        self._news2vec_embedding_path = embedding_path
        self._export_features = export_features
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
        article_node_values_df = word_edges_df.withColumnRenamed('id1', 'id') \
                                        .withColumn('rank',sfuncs.row_number().over(w)) \
                                        .where(sfuncs.col('rank') <= TOP_NUM_NODE) \
                                        .groupby('id') \
                                        .pivot('rank') \
                                        .agg(sfuncs.max('word'))


        article_nodes_df = article_nodes_df.join(sfuncs.broadcast(article_node_values_df), 'id')

        article_nodes_df = article_nodes_df.drop('word')

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

        # get article sentiment
        sentiment_pipeline = PretrainedPipeline("classifierdl_bertwiki_finance_sentiment_pipeline", lang = "en")
        article_nodes_df = sentiment_pipeline.annotate(article_nodes_df, 'text') \
                                             .select('*', sfuncs.col('class.result').getItem(0).alias('sentiment_output')) \
                                             .drop('document', 'sentence_embeddings', 'class')
        article_nodes_df = article_nodes_df.withColumn('sentiment', sfuncs.when(sfuncs.col('sentiment_output') == 'positive', 'positive_1') \
                                                                          .when(sfuncs.col('sentiment_output') == 'neutral', 'neutral_1') \
                                                                          .when(sfuncs.col('sentiment_output') == 'negative', 'negative_1'))

        # get month 
        article_nodes_df = article_nodes_df.withColumn('month', sfuncs.concat(sfuncs.lit('m_'), sfuncs.month('publish_date')))
        # get day of month
        article_nodes_df = article_nodes_df.withColumn('day_of_month', sfuncs.concat(sfuncs.lit('d_'), sfuncs.dayofmonth('publish_date')))
        # get of week
        article_df = article_nodes_df.withColumn('day_of_week', sfuncs.concat(sfuncs.lit('wd_'), sfuncs.dayofweek('publish_date')))

        # load embeddings from file
        spark = spark_manager.get_spark_session()
        embeddings_df = spark.read.csv(self._news2vec_embedding_path, header=True)

        # sort out columns
        embeddings_df = embeddings_df.withColumnRenamed('_c0', 'token')

        embed_col_names = embeddings_df.columns
        embed_col_names.remove('token')
        embeddings_df = embeddings_df.withColumn('token_embedding', sfuncs.array([sfuncs.col(col_name) for col_name in embed_col_names]))
        embeddings_df = embeddings_df.drop(*embed_col_names)

        # creating article embedding
        feature_cols = [str(num) for num in range(1, TOP_NUM_NODE + 1)] + ['num_words_ord', 'sentiment', 'month', 'day_of_week', 'day_of_month']
        feature_vals = {
            'num_words_ord': ['wc200', 'wc500', 'wc1000', 'wc2000', 'wc3000', 'wc5000', 'wcmax'],
            'sentiment': ['neutral', 'negative', 'positive'],
            'month': [f"m_{month}" for month in range(1, 13)],
            'day_of_month': [f"d_{day}" for day in range(1, 32)],
            'day_of_week': [f"wd_{day}" for day in range(1, 8)]
        }

        embeddings_df = accumulate_embeddings(article_df, embeddings_df, feature_cols, len(embed_col_names), feature_vals)

        article_df = article_df.select('id', 'title', 'text', 'publish_date', 'url')
        article_df = article_df.join(embeddings_df, 'id')
        article_df = article_df.select('title', 'text', 'publish_date', 'url', 'embedding')
        return article_df



    def _process(self, spark_manager):
        res = self.input._process(spark_manager)

        if self._do_news2vec_embed:
            return self._news2vec_embed(res, spark_manager)
