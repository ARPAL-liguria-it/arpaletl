PYTHON = python
PIP = pip
PACKAGE_NAME = arpaletl

dependencies:
	$(PIP) install -r requirements.txt

clean:
	rm -rf ./dist/
	rm -rf ./arpaletl.egg-info/
	rm -rf ./build/

build: clean dependencies
	$(PYTHON) setup.py bdist_wheel

install:
	$(PIP) install ./dist/arpaletl*
