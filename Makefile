setup: requirements.txt
	pip install -r requirements.txt
	python -m spacy download en_core_web_sm

test:
	pytest