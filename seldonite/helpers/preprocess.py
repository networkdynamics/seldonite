
import spacy

class Preprocessor:

    def __init__(self):
        self.nlp = spacy.load('en_core_web_sm', disable=['parser', 'tagger', 'ner'])
    
    def preprocess(self, docs):
        for doc in docs:
            doc = doc.lower()
            # TODO use spacy pipe for faster processing
            # TODO use multicore for performance
            doc = self.nlp(doc)
            lemmatized = []
            for word in doc:
                lemma = word.lemma_.strip()
                if lemma:
                    token = lemma
                else:
                    token = word

                lemmatized.append(lemma)
                
            yield lemmatized

