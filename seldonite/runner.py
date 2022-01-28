from seldonite.spark import spark_tools

class Runner():
    def __init__(self, input, master_url=None, num_executors=1, executor_cores=16, executor_memory='160g', spark_conf={}):
        self.input = input

        self.spark_master_url = master_url
        self.num_executors = num_executors
        self.executor_cores = executor_cores
        self.executor_memory = executor_memory
        self.spark_conf = spark_conf

    def to_pandas(self):
        '''
        :return: Pandas dataframe
        '''
        self._check_args()

        spark_builder = self._get_spark_builder()
        with spark_builder.start_session() as spark_manager:
            df = self.input._process(spark_manager)
            df = df.toPandas()

        return df

    def send_to_database(self, connection_string, database, table):
        self._check_args()
        spark_builder = self._get_spark_builder()
        spark_builder.set_output_database(connection_string)
        with spark_builder.start_session() as spark_manager:
            df = self.input._process(spark_manager)
            df.write \
                .format("mongo") \
                .mode("append") \
                .option("database", database) \
                .option("collection", table) \
                .save()

    def _get_spark_builder(self):
        spark_builder = spark_tools.SparkBuilder(self.spark_master_url, use_bigdl=use_bigdl, archives=archives,
                                                 executor_cores=self.executor_cores, executor_memory=self.executor_memory, num_executors=self.num_executors,
                                                 spark_conf=self.spark_conf)