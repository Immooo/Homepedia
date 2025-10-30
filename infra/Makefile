SHELL := /bin/bash
COMPOSE := docker compose -f infra/docker-compose.yml --env-file .env

.PHONY: help build rebuild up down logs ps health sh-app init-db etl-ls etl-valeurs etl-insee etl-agg etl-spark

help:
	@echo "Targets: build, rebuild, up, down, logs, ps, health, sh-app, init-db, etl-*"

build:
	$(COMPOSE) build

rebuild:
	$(COMPOSE) build --no-cache

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f --tail=200

ps:
	$(COMPOSE) ps

health:
	curl -s http://localhost:$${STREAMLIT_SERVER_PORT:-8501}/_stcore/health || exit 1

sh-app:
	$(COMPOSE) exec app bash

init-db:
	$(COMPOSE) exec app python -c "import os,sqlite3; db=os.getenv('DB_PATH','data/homepedia.db'); \
	    os.makedirs(os.path.dirname(db), exist_ok=True); con=sqlite3.connect(db); \
	    con.execute('CREATE TABLE IF NOT EXISTS _bootstrap (id INTEGER PRIMARY KEY, created_at TEXT)'); \
	    con.commit(); con.close(); print('DB ready at',db)"

# Exécutions ETL à la demande (adapte si besoin)
etl-ls:
	@echo "ETL disponibles : valeursfoncieres, insee, agg, spark"

etl-valeurs:
	$(COMPOSE) exec app python src/backend/ingest_valeursfoncieres.py

etl-insee:
	$(COMPOSE) exec app bash -lc "python src/backend/ingest_insee_population.py && python src/backend/ingest_insee_income.py && python src/backend/ingest_insee_unemployment.py && python src/backend/ingest_insee_poverty.py && python src/backend/ingest_insee_region.py"

etl-agg:
	$(COMPOSE) exec app python src/backend/aggregate_by_region.py

etl-spark:
	$(COMPOSE) exec app python src/backend/spark_dvf_analysis.py
