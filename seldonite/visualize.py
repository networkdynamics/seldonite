from seldonite import base

class Visualize(base.BaseStage):
    def __init__(self, input):
        super().__init__(input)

    def show_graph(self):
        

    def _process(self, spark_manager):
        res = self.input._process(spark_manager)

