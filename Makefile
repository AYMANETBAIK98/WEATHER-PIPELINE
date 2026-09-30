.PHONY: up down logs test lint psql run

up:            ## start warehouse + Airflow (UI on http://localhost:8080)
	docker compose up -d --build

down:          ## stop everything (data volumes are kept)
	docker compose down

logs:
	docker compose logs -f airflow

test:          ## run tests against the dockerised warehouse
	TEST_DATABASE_DSN="host=localhost port=5433 dbname=weather user=etl password=etl" pytest -v

lint:
	ruff check .

psql:          ## open a SQL shell on the warehouse
	docker compose exec warehouse psql -U etl -d weather

run:           ## run yesterday's load without Airflow
	WAREHOUSE_PORT=5433 python scripts/run_pipeline.py
