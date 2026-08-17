.PHONY: up down migrate build fmt fmt-check lint test test-unit db-test gate run-api run-worker run-fake-charge

DATABASE_URL ?= postgres://reliableq:reliableq@localhost:5432/reliableq

up:
	docker compose up -d postgres

down:
	docker compose down

migrate:
	DATABASE_URL=$(DATABASE_URL) cargo run -p reliableq-db --bin migrate

build:
	cargo build --workspace

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

lint:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

# Runs everything that does not require a database (config/domain unit tests).
test-unit:
	cargo test --workspace --lib

# Runs the full suite, including db/integration/chaos tests that need
# `make up` to have provisioned postgres first.
test:
	DATABASE_URL=$(DATABASE_URL) cargo test --workspace --all-features

# Full local quality gate, matching .github/workflows/ci.yml and
# SPEC.md sec. 17.
gate: fmt-check lint test

run-api:
	DATABASE_URL=$(DATABASE_URL) cargo run -p reliableq-api

run-worker:
	DATABASE_URL=$(DATABASE_URL) cargo run -p reliableq-worker

run-fake-charge:
	DATABASE_URL=$(DATABASE_URL) cargo run -p fake-charge
