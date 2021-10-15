setup: requirements.txt
	pip install -r requirements.txt
	pip install --edit .

test:
	pytest