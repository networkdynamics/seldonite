from seldonite import source

class MockSource(source.Source):
    def __init__(self):
        super().__init__()

    def fetch(self):
        return []