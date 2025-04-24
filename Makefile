.PHONY: check fix freeze


init-dev:
	pip install -r dev.txt -r requirements.txt


freeze:
	@echo "Freezing dependencies..."
	uv lock --frozen
	uv export --group dev --no-hashes --output-file dev.txt
	uv export --no-hashes --output-file requirements.txt


# Default target
.DEFAULT_GOAL := check
