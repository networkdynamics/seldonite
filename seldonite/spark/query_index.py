from seldonite.spark.sparkcc import CCIndexSparkJob

class QueryIndexJob(CCIndexSparkJob):

    name = "QueryIndexJob"

    def process_dataset(self, dataset):
        return dataset.toPandas()