-- Generated from SQL/SQL_ORACULO_BACKUP.sql by tools/split_pg_dump_bootstrap.py

CREATE SCHEMA binance_futures;

CREATE SCHEMA binance_spot;

CREATE SCHEMA deribit;

CREATE SCHEMA oraculo;

CREATE SCHEMA oraculo_audit;

CREATE SCHEMA oraculo_bt;

CREATE DOMAIN public.instrument_id_t AS text;

CREATE TYPE public.side_t AS ENUM (
    'buy',
    'sell'
);

CREATE TYPE public.action_t AS ENUM (
    'insert',
    'update',
    'delete'
);

CREATE TYPE public.severity_t AS ENUM (
    'ALTA',
    'MEDIA',
    'BAJA'
);
