.PHONY: pypi

pypi:
	rm dist/*
	python setup.py sdist
	twine upload --config-file=.pypirc dist/*
