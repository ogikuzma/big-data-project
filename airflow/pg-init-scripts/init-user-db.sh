#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
	CREATE USER aiflow WITH PASSWORD 'postgres';
	CREATE DATABASE aiflow;
	GRANT ALL PRIVILEGES ON DATABASE aiflow TO aiflow;
EOSQL