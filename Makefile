# Makefile for BigQuery SQL Antipattern Checker

.PHONY: help install install-dev clean lint format type-check test test-cov security docs build check-all

# Default target
help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Installation
install: ## Install the package
	pip install -e .

install-dev: ## Install development dependencies
	pip install -e .[dev]
	pre-commit install
	pre-commit install --hook-type commit-msg

# Cleaning
clean: ## Clean build artifacts
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	find . -type d -name __pycache__ -delete
	find . -type f -name "*.pyc" -delete

# Code quality
lint: ## Run linting with ruff
	ruff check src tests

lint-fix: ## Run linting with ruff and fix issues
	ruff check --fix src tests

format: ## Format code with ruff
	ruff format src tests

format-check: ## Check code formatting
	ruff format --check src tests

type-check: ## Run type checking with mypy
	mypy src/

# Testing
test: ## Run tests
	pytest

test-watch: ## Run tests in watch mode
	pytest --watch

# Security
security: ## Run security checks
	bandit -r src/
	safety check

# Documentation
docs: ## Check documentation coverage
	interrogate src/

# Dead code detection
dead-code: ## Find dead code with vulture
	vulture src/

# Build
build: ## Build the package
	python -m build

# Comprehensive checks
check-all: lint type-check test security docs ## Run all checks

# Pre-commit
pre-commit: ## Run pre-commit hooks on all files
	pre-commit run --all-files

# Development workflow
dev-setup: clean install-dev ## Setup development environment
	@echo "Development environment setup complete!"
	@echo "Run 'make check-all' to verify everything is working."

# Release workflow
release-check: clean check-all build ## Check if ready for release
	@echo "Release check complete!"

# Quick development commands
quick-check: lint-fix format type-check ## Quick development check
	@echo "Quick check complete!"

# Install and run the CLI
run-create-config: ## Create a config file using the CLI
	bq-antipattern-checker create-config

run-list-antipatterns: ## List available antipatterns
	bq-antipattern-checker list-antipatterns

run-help: ## Show CLI help
	bq-antipattern-checker --help