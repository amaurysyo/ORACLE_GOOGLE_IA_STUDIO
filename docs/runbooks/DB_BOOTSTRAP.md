# Database bootstrap runbook

## Prerequisitos

- Postgres instalado localmente (cliente `psql` disponible).
- TimescaleDB instalado y la extensión `timescaledb` disponible en la instancia de Postgres.
- Variable de entorno `PG_DSN` configurada apuntando a la base de datos de destino (incluye credenciales con permisos para crear esquemas, tipos y extensiones).

## Pasos para crear la estructura

Ejecuta los scripts en orden usando el DSN configurado:

```bash
psql "$PG_DSN" -f SQL/bootstrap/00_extensions.sql
psql "$PG_DSN" -f SQL/bootstrap/10_core_schema.sql
psql "$PG_DSN" -f SQL/bootstrap/20_core_tables.sql
psql "$PG_DSN" -f SQL/bootstrap/30_core_functions_views.sql
psql "$PG_DSN" -f SQL/bootstrap/40_timescale.sql
```

> Sugerencia: usa la misma versión de Postgres/TimescaleDB con la que se generaron los scripts para evitar diferencias en tipos o compatibilidad de funciones.

## Cómo regenerar el dump (informativo)

Si necesitas volver a generar el dump de esquema sin metadatos de permisos ni propietarios:

```bash
pg_dump --schema-only --no-owner --no-privileges --format=p --encoding=UTF8 --file SQL/oraculo_schema_only.sql "$PG_DSN"
python tools/split_pg_dump_bootstrap.py
python tools/export_timescale_layer.py
```

El archivo `SQL/bootstrap/40_timescale.sql` es generado por `tools/export_timescale_layer.py` y debe conservarse sin ser sobrescrito por el splitter.

## Verificación mínima

Conéctate con `psql "$PG_DSN"` y valida:

1) **Esquemas creados**

```sql
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name IN ('binance_futures', 'deribit', 'oraculo', 'oraculo_bt')
ORDER BY 1;
```

2) **Tablas clave disponibles** (ejemplos representativos)

```sql
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN ('oraculo', 'oraculo_audit', 'binance_futures', 'deribit')
  AND table_name IN (
    'metrics_series', 'slice_events', 'rule_alerts',
    'orderbook_snapshots', 'mark_funding', 'trades',
    'options_iv_surface', 'options_trades'
  )
ORDER BY 1, 2;
```

3) **Sin propietarios/permisos asumidos en el dump**

```sql
SELECT table_schema, table_name, tableowner
FROM information_schema.tables
WHERE table_schema IN ('binance_futures', 'deribit', 'oraculo', 'oraculo_bt')
ORDER BY 1, 2
LIMIT 15;

SELECT *
FROM information_schema.table_privileges
WHERE table_schema IN ('binance_futures', 'deribit', 'oraculo', 'oraculo_bt')
  AND grantee NOT IN (current_user)
LIMIT 5;
```

4) **Extensión de TimescaleDB cargada (opcional pero recomendado)**

```sql
SELECT extname FROM pg_extension WHERE extname = 'timescaledb';
```

Si las consultas anteriores devuelven filas para los esquemas y tablas, y no hay privilegios inesperados, el bootstrap quedó aplicado correctamente.
