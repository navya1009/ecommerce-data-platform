# Makefile
# -----------------------------------------------------------------------------
# Common development tasks.
# Usage: make <target>
# -----------------------------------------------------------------------------

.PHONY: help test-unit test-integration test-all coverage lint clean install

# Override the interpreter, e.g. make PYTHON=venv/bin/python test-unit
PYTHON ?= python
PYTEST := $(PYTHON) -m pytest

help:
	@echo "Available targets:"
	@echo "  install            Install dependencies"
	@echo "  lint               Run pre-commit hooks"
	@echo "  test-unit          Run unit tests"
	@echo "  test-integration   Run integration tests (starts Docker DBs)"
	@echo "  test-all           Run all tests"
	@echo "  coverage           Run tests with HTML coverage report"
	@echo "  clean              Remove caches"

install:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt -r requirements-dev.txt

lint:
	pre-commit run --all-files

test-unit:
	$(PYTEST) tests/unit/ -v

test-integration:
	$(PYTEST) tests/integration/ -v --timeout=600

test-all:
	$(PYTEST) tests/ -v --timeout=600

coverage:
	$(PYTEST) tests/unit/ \
		--cov=src \
		--cov-report=term-missing \
		--cov-report=html:htmlcov \
		--cov-report=xml:coverage.xml
	@echo "HTML report: htmlcov/index.html"

clean:
	rm -rf .pytest_cache htmlcov .coverage coverage.xml
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true