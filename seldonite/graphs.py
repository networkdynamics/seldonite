from html import entities
import networkx as nx
import pyspark.sql.functions as sfuncs
import pyspark.sql as psql
from sparknlp.pretrained import PretrainedPipeline

from seldonite import base

class Graph(base.BaseStage):

    def __init__(self, input):
        super().__init__(input)
        self.graph_option = ''

    def build_news2vec_graph(self, export_articles=False):
        self.graph_option = 'news2vec'
        self.export_articles = export_articles
        return self

    def build_entity_dag(self):
        self.graph_option = 'entity_dag'
        return self

    def _build_news2vec_graph(self, df: psql.DataFrame, spark_manager):

        Z1 = 1
        Z2 = 2
        TOP_NUM_NODE = 20

        df.cache()

        # explode tfidf into rows and get unique word nodes
        words_df = df.select('id', sfuncs.explode(sfuncs.col('text_top_n')).alias('map')) \
                     .union(df.select('id', sfuncs.explode(sfuncs.col('title_top_n')).alias('map'))) \
                     .select('id', sfuncs.col('map.word').alias('word'))

        # get words which appear in multiple articles
        words_df = words_df.drop_duplicates(['id', 'word']) \
                           .groupby('word') \
                           .count()

        words_df = words_df.where(sfuncs.col('count') > 1) \
                           .select('word')

        # create distinct ids for each node
        nodes_df = df.drop('id') \
                     .unionByName(words_df, allowMissingColumns=True) \
                     .withColumn('id', sfuncs.monotonically_increasing_id())

        nodes_df.cache()

        # explode tfidf again to get edges
        article_nodes_df = nodes_df.where(sfuncs.col('text_top_n').isNotNull() & sfuncs.col('title_top_n').isNotNull())

        text_edges_df = article_nodes_df.select('id', sfuncs.explode(sfuncs.col('text_top_n')).alias('text')) \
                                        .select(sfuncs.col('id').alias('text_id'), sfuncs.col('text.word').alias('text_word'), sfuncs.col('text.value').alias('text_value'))
        title_edges_df = article_nodes_df.select('id', sfuncs.explode(sfuncs.col('title_top_n')).alias('title')) \
                                         .select(sfuncs.col('id').alias('title_id'), sfuncs.col('title.word').alias('title_word'), sfuncs.col('title.value').alias('title_value'))

        # combine title and text edges
        edges_df = text_edges_df.join(title_edges_df, 
                                      on=((text_edges_df['text_id'] == title_edges_df['title_id']) & (text_edges_df['text_word'] == title_edges_df['title_word'])), 
                                      how='full') \
                                .select(sfuncs.coalesce('title_id', 'text_id').alias('id1'), sfuncs.coalesce('text_word', 'title_word').alias('word'), 'text_value', 'title_value')
        edges_df.cache()

        # divide each weight by scalar
        edges_df = edges_df.select('id1', 'word', (sfuncs.col('title_value') / Z1).alias('title_value'), (sfuncs.col('text_value') / Z2).alias('text_value'))

        # add up values
        word_edges_df = edges_df.select('id1', 'word', (sfuncs.coalesce('title_value', sfuncs.lit(0)) + sfuncs.coalesce('text_value', sfuncs.lit(0))).alias('weight'))

        # map word to node id
        word_nodes_df = nodes_df.where(sfuncs.col('word').isNotNull()) \
                                .select('id', 'word')
        edges_df = word_edges_df.join(word_nodes_df, on='word') \
                                .select('id1', sfuncs.col('id').alias('id2'), 'weight')
        
        # drop edges with weight 0
        edges_df = edges_df.where(sfuncs.col('weight') > 0)

        # get node values
        # node values for word nodes
        word_nodes_df = word_nodes_df.select('id', sfuncs.col('word').alias('value'))

        # get rid of word nodes with no edges (possibly due to 0 weight edges being removed)
        word_nodes_df = word_nodes_df.join(edges_df, (word_nodes_df['id'] == edges_df['id1']) | (word_nodes_df['id'] == edges_df['id2']), 'inner') \
                                     .select('id', 'value') \
                                     .drop_duplicates(['id', 'value'])

        # node values for article nodes

        # concat all top tfidf words
        w = psql.Window.partitionBy('id').orderBy(sfuncs.desc('weight'))
        article_node_values_df = word_edges_df.withColumnRenamed('id1', 'id') \
                                        .withColumn('rank',sfuncs.row_number().over(w)) \
                                        .where(sfuncs.col('rank') <= TOP_NUM_NODE) \
                                        .groupby('id') \
                                        .agg(sfuncs.concat_ws(",", sfuncs.collect_list(sfuncs.col('word'))).alias('top_tfidf'))

        article_nodes_df = article_nodes_df.join(article_node_values_df, 'id')
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

        # get article sentiment
        sentiment_pipeline = PretrainedPipeline("classifierdl_bertwiki_finance_sentiment_pipeline", lang = "en")
        article_nodes_df = sentiment_pipeline.annotate(article_nodes_df, 'text') \
                                             .select('*', sfuncs.col('class.result').getItem(0).alias('sentiment')) \
                                             .drop('document', 'sentence_embeddings', 'class')

        # get month 
        article_nodes_df = article_nodes_df.withColumn('month', sfuncs.concat(sfuncs.lit('m_'), sfuncs.month('publish_date')))
        # get day of month
        article_nodes_df = article_nodes_df.withColumn('day_of_month', sfuncs.concat(sfuncs.lit('d_'), sfuncs.dayofmonth('publish_date')))
        # get of week
        article_nodes_df = article_nodes_df.withColumn('day_of_week', sfuncs.concat(sfuncs.lit('wd_'), sfuncs.dayofweek('publish_date')))

        # compile into column
        article_nodes_df = article_nodes_df.withColumn('value', sfuncs.concat_ws(',', 
                                                                                 sfuncs.col('top_tfidf'), 
                                                                                 sfuncs.col('num_words_ord'), 
                                                                                 sfuncs.col('sentiment'), 
                                                                                 sfuncs.col('month'), 
                                                                                 sfuncs.col('day_of_month'), 
                                                                                 sfuncs.col('day_of_week')))

        node_map_df = word_nodes_df.union(article_nodes_df.select('id', 'value'))

        # convert to networkx format and collect
        edges = edges_df.rdd.map(lambda row: (row['id1'], row['id2'], row['weight'])).collect()

        if self.export_articles:
            # get nodes dataframe in pandas
            nodes_df = nodes_df.toPandas()

        # create nx graph
        graph = nx.Graph()
        graph.add_weighted_edges_from(edges)

        # create mapping pandas dataframe
        node_map_df = node_map_df.toPandas()

        return graph, node_map_df

    def _build_entity_dag(self, df: psql.DataFrame, spark_manager):
        df.cache()

        # explode entities
        entities_df = df.select('*', sfuncs.explode(sfuncs.col('entities')).alias('entities')) \
                        .select('url', 'text', 'title', 'publish_date', sfuncs.col('entities.entity').alias('entitiy'), sfuncs.col('entities.type').alias('entity_type'))

        # find shared entity edges
        edges_df = entities_df.alias('df1').join(entities_df.alias('df2'), \
                                                 (sfuncs.col('df1.entity') == sfuncs.col('df2.entity')) & (sfuncs.col('df1.entity_type') == sfuncs.col('df2.entity_type')) & (sfuncs.col('df1.publish_date') > sfuncs.col('df2.publish_date')), \
                                                 'inner')

        # find day diff between news events
        edges_df = edges_df.withColumn('date_diff', )

    def _set_spark_options(self, spark_builder):
        spark_builder.use_spark_nlp()
        self.input._set_spark_options(spark_builder)

    def _process(self, spark_manager):
        df = self.input._process(spark_manager)

        if self.graph_option == 'news2vec':
            graph = self._build_news2vec_graph(df, spark_manager)
        elif self.graph_option == 'entity_dag':
            graph = self._build_entity_dag(df, spark_manager)
        else:
            raise ValueError('Must have chosen graph option with this pipeline step')

        return graph