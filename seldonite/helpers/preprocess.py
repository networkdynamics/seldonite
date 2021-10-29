
import spacy

class Preprocessor:

    def __init__(self):
        self.nlp = spacy.load('en_core_web_sm', disable=['tok2vec', 'parser', 'ner'])
    
    def preprocess(self, docs):
        for doc in docs:
            doc = doc.lower()
            # TODO use spacy pipe for faster processing
            # TODO use multicore for performance
            doc = self.nlp(doc)
            lemmatized = []
            for word in doc:
                if word.is_punct or word.is_stop:
                    continue

                if not word.is_alpha:
                    continue

                lemma = word.lemma_.strip()
                lemmatized.append(lemma)
                
            yield lemmatized

