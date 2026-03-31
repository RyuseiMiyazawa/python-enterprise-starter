PYTHONPATH=src

.PHONY: test demo lint

test:
	PYTHONPATH=$(PYTHONPATH) python3 -m unittest discover -s tests -p 'test_*.py'

demo:
	PYTHONPATH=$(PYTHONPATH) python3 -m enterprise_starter

lint:
	PYTHONPATH=$(PYTHONPATH) python3 -m compileall src tests

