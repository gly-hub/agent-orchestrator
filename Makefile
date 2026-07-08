.PHONY: check compile lint typecheck test

check: compile lint typecheck test

compile:
	PYTHONPATH=src python3 -m compileall -q src tests

lint:
	python3 -m ruff check src tests

typecheck:
	python3 -m pyright src

test:
	PYTHONPATH=src python3 -m pytest tests/ --cov=agent_orchestrator --cov-report=term-missing
