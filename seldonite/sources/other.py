from seldonite import base
from seldonite.spark import spark_tools

class GraphCSV:
    def __init__(self, node_path, edge_path):
        super().__init__()
        self.node_path = node_path
        self.edge_path = edge_path

    def _process(self, spark_manager):
        spark = spark_manager.get_spark_session()

        nodes_df = spark.read.csv(self.node_path, inferSchema=True, header=True, multiLine=True, escape='"')
        edges_df = spark.read.csv(self.edge_path, inferSchema=True, header=True, multiLine=True, escape='"')

        return nodes_df, edges_df

    def _set_spark_options(self, spark_builder: spark_tools.SparkBuilder):
        return