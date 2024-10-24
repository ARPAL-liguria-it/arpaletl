PYTHON = python
PIP = pip
PACKAGE_NAME = arpaletl

env:
	python3 -m venv .env

dependencies: env
	source .env/bin/activate && $(PIP) install -r ./requirements.txt

clean:
	rm -rf ./.env/
	rm -rf ./dist/
	rm -rf ./arpaletl.egg-info/
	rm -rf ./build/

build: clean dependencies
	source .env/bin/activate && $(PYTHON) setup.py bdist_wheel

install:
	$(PIP) install ./dist/arpaletl*
