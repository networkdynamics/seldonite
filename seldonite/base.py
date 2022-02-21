from __future__ import annotations

from seldonite.spark import spark_tools

class BaseStage:
    input: BaseStage

    def __init__(self, input: BaseStage):
        self.input = input

    def _process(self, spark_manager: spark_tools.SparkManager):
        raise NotImplementedError('Stage must implement _process method')

    def _set_spark_options(self, spark_builder: spark_tools.SparkBuilder):
        self.input._set_spark_options(spark_builder)