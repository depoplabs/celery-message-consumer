.PHONY: pypi, tag, test

pypi:
	rm -f dist/*
	python setup.py sdist
	twine upload --config-file=.pypirc dist/*
	make tag

tag:
	git tag $$(python event_consumer/__about__.py)
	git push --tags

test:
	PYTHONPATH=. py.test -v -s --pdb tests/
