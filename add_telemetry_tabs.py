# -*- coding: utf-8 -*-
"""
Created on Wed Nov 19 16:38:25 2025

@author: AMAURY
"""

import pandas as pd
from pathlib import Path

# Rutas de TUS archivos (ajústalas si los tienes en otra carpeta)
path1 = Path(r"E:\OneDrive\Proyecto Binance Margin Boot\Proyecto Oraculo BTC\oraculo_docs_actualizados - pruebas\Reglas_R1_R22_dependencias_corregidas.xlsx")
path2 = Path(r"E:\OneDrive\Proyecto Binance Margin Boot\Proyecto Oraculo BTC\oraculo_docs_actualizados - pruebas\Resumen_impl_vs_excel.xlsx")

schema_rows = [
    ("ts_bucket", "timestamptz", "Minuto (UTC) de agregación."),
    ("instrument_id", "text", "ID canónico del instrumento (p.ej., BINANCE:PERP:BTCUSDT)."),
    ("profile", "text", "Perfil de reglas/mercado (p.ej., EU)."),
    ("rule", "text", "Código de regla (R3, R4, ..., R20)."),
    ("side", "text", "Lado asociado ('buy', 'sell' o 'na')."),
    ("emitted", "integer", "Alertas emitidas (tras upsert_rule_alert)."),
    ("disc_abs_no_best", "integer", "Descartes por falta de best bid/ask para absorción."),
    ("disc_bw_basis", "integer", "Descartes BreakWall por basis_vel insuficiente."),
    ("disc_bw_dep_refill", "integer", "Descartes BreakWall por depleción/refill insuficiente."),
    ("disc_dom_spread", "integer", "Descartes Dominance por spread > umbral."),
    ("disc_metrics_none", "integer", "Descartes de disparadores métricos (R16–R20) por no alcanzar umbral."),
]
df_schema = pd.DataFrame(schema_rows, columns=["column", "pg_type", "description"])

df_template = pd.DataFrame(columns=[
    "ts_bucket", "instrument_id", "profile", "rule", "side",
    "emitted", "disc_abs_no_best", "disc_bw_basis",
    "disc_bw_dep_refill", "disc_dom_spread", "disc_metrics_none"
])

queries = [
    ("Última hora por regla y lado",
     """SELECT ts_bucket, rule, side, SUM(emitted) AS emitted,
       SUM(disc_abs_no_best) AS disc_abs_no_best,
       SUM(disc_bw_basis) AS disc_bw_basis,
       SUM(disc_bw_dep_refill) AS disc_bw_dep_refill,
       SUM(disc_dom_spread) AS disc_dom_spread,
       SUM(disc_metrics_none) AS disc_metrics_none
FROM oraculo.rule_telemetry
WHERE ts_bucket >= now() - interval '60 minutes'
  AND instrument_id = 'BINANCE:PERP:BTCUSDT'
  AND profile = 'EU'
GROUP BY 1,2,3
ORDER BY 1 DESC, 2, 3;"""),
    ("Hoy por regla (lado agregado)",
     """SELECT rule,
       SUM(emitted) AS emitted,
       SUM(disc_abs_no_best + disc_bw_basis + disc_bw_dep_refill + disc_dom_spread + disc_metrics_none) AS discards
FROM oraculo.rule_telemetry
WHERE ts_bucket::date = current_date
  AND instrument_id = 'BINANCE:PERP:BTCUSDT'
  AND profile = 'EU'
GROUP BY rule
ORDER BY rule;"""),
    ("Top 10 minutos por número de alertas",
     """SELECT ts_bucket, SUM(emitted) AS emitted
FROM oraculo.rule_telemetry
WHERE instrument_id = 'BINANCE:PERP:BTCUSDT'
  AND profile = 'EU'
GROUP BY ts_bucket
ORDER BY emitted DESC
LIMIT 10;"""),
    ("Distribución de descartes por causa (últimas 24h)",
     """SELECT
  SUM(disc_abs_no_best) AS abs_no_best,
  SUM(disc_bw_basis) AS bw_basis,
  SUM(disc_bw_dep_refill) AS bw_dep_refill,
  SUM(disc_dom_spread) AS dom_spread,
  SUM(disc_metrics_none) AS metrics_none
FROM oraculo.rule_telemetry
WHERE ts_bucket >= now() - interval '24 hours'
  AND instrument_id = 'BINANCE:PERP:BTCUSDT'
  AND profile = 'EU';"""),
]
df_queries = pd.DataFrame(queries, columns=["title", "sql"])

howto_text = [
    ("1. Poblar tabla", "El runner crea/upserta en oraculo.rule_telemetry automáticamente."),
    ("2. Exportar datos", "Ejecuta una de las SQL de la pestaña 'telemetry_queries' y exporta a CSV."),
    ("3. Importar a Excel", "Datos > Desde texto/CSV (UTF-8) o Datos > Desde base de datos PostgreSQL."),
    ("4. Crear tabla", "Selecciona el rango y conviértelo en Tabla (Ctrl+T)."),
    ("5. Crear pivots", "Inserta Tabla Dinámica por regla, lado, buckets de tiempo, etc."),
    ("Notas", "Las columnas *_disc_* son causas de descarte agregadas por minuto."),
]
df_howto = pd.DataFrame(howto_text, columns=["step", "details"])

def add_tabs(xlsx_path: Path):
    if not xlsx_path.exists():
        # Si no existe, lo creamos desde cero.
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            df_schema.to_excel(writer, sheet_name="telemetry_schema", index=False)
            df_template.to_excel(writer, sheet_name="telemetry_template", index=False)
            df_queries.to_excel(writer, sheet_name="telemetry_queries", index=False)
            df_howto.to_excel(writer, sheet_name="telemetry_howto", index=False)
        return

    # Si existe, añadimos/reemplazamos las pestañas
    with pd.ExcelWriter(xlsx_path, mode="a", engine="openpyxl", if_sheet_exists="replace") as writer:
        df_schema.to_excel(writer, sheet_name="telemetry_schema", index=False)
        df_template.to_excel(writer, sheet_name="telemetry_template", index=False)
        df_queries.to_excel(writer, sheet_name="telemetry_queries", index=False)
        df_howto.to_excel(writer, sheet_name="telemetry_howto", index=False)

add_tabs(path1)
add_tabs(path2)

print("Listo: pestañas de telemetría añadidas/actualizadas en:")
print(" -", path1)
print(" -", path2)
