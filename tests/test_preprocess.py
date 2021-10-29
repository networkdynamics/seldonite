from seldonite.helpers import preprocess

import pytest

@pytest.mark.parametrize("docs, cleaned",
    [(["It's 2019, and we need #revolution"], [["need", "revolution"]])])
def test_preprocess(docs, cleaned):
    preprocessor = preprocess.Preprocessor()
    processed_docs = list(preprocessor.preprocess(docs))

    assert len(processed_docs) == len(cleaned)
    for idx in range(len(processed_docs)):
        assert processed_docs[idx] == cleaned[idx]

