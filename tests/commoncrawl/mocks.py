
class MockRDD:
    def __init__(self, df):
        self.df = df

    def __iter__(self):
        for index, row in self.df.iterrows():
            yield row

class MockAccumulator:
    def __init__(self):
        self.cnt = 0

    def add(self, x):
        self.cnt += x