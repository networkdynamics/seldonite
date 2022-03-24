from contextlib import contextmanager

from seldonite import base
from seldonite.spark import spark_tools

class Runner():
    def __init__(self, input: base.BaseStage, master_url=None, num_executors=1, executor_cores=16, executor_memory='128g', driver_cores=2, driver_memory='128g', python_executable='python', spark_conf={}):
        assert isinstance(input, base.BaseStage), "Input must be a Seldonite stage."
        self.input = input

        self.spark_master_url = master_url
        self.num_executors = num_executors
        self.executor_cores = executor_cores
        self.executor_memory = executor_memory
        self.driver_memory = driver_memory
        self.driver_cores = driver_cores
        self.spark_conf = spark_conf
        self.python_executable = python_executable

    def get_obj(self):
        with self.start_and_process() as obj:
            return obj
        
    @contextmanager
    def start_and_process(self):
        spark_builder = self._get_spark_builder()
        with spark_builder.start_session() as spark_manager:
            df = self.input._process(spark_manager)
            yield df

        return df

    def to_pandas(self):
        '''
        :return: Pandas dataframe
        '''

        with self.start_and_process() as df:
            df = df.toPandas()

        return df

    def send_to_database(self, connection_string, database, table):
        spark_builder = self._get_spark_builder()
        spark_builder.set_conf('spark.mongodb.output.uri', connection_string)
        spark_builder.add_package('org.mongodb.spark:mongo-spark-connector_2.12:3.0.1')
        spark_builder.set_conf('spark.mongodb.keep_alive_ms', 20000)
        with spark_builder.start_session() as spark_manager:
            df = self.input._process(spark_manager)

            for df_batch in spark_tools.batch(df, max_rows=1000000):
                df_batch.write \
                    .format("mongo") \
                    .mode("append") \
                    .option("database", database) \
                    .option("collection", table) \
                    .save()

    def _get_spark_builder(self):
        spark_builder = spark_tools.SparkBuilder(self.spark_master_url, executor_cores=self.executor_cores, executor_memory=self.executor_memory, 
                                                 num_executors=self.num_executors, driver_cores=self.driver_cores, driver_memory=self.driver_memory, 
                                                 python_executable=self.python_executable, spark_conf=self.spark_conf)

        self.input._set_spark_options(spark_builder)

        return spark_builder