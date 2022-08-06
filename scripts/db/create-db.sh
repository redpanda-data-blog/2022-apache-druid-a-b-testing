#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
	CREATE USER druid WITH PASSWORD 'druid';
	ALTER ROLE druid LOGIN;
	CREATE DATABASE druid with owner druid;

EOSQL