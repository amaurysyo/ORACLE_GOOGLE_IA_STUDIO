# Runbook — Deribit Surface Builder (options_iv_surface)

## Propósito
Construir de forma incremental una surface IV 1m a partir de `deribit.options_ticker` + `deribit.options_instruments`, llenando `deribit.options_iv_surface` (tenor + moneyness) y habilitando `oraculo.iv_surface_1m` para el detector `skew_shock` (R35).

## Configuración
`config/config.yaml`
```yaml
deribit_surface_builder:
  enabled: false         # mantener apagado hasta calibrar
  poll_s: 30             # frecuencia de ejecución
  lookback_s: 600        # ventana para buscar snapshots recientes
  lag_s: 60              # construir el minuto cerrado (now-60s)
  underlying: "BTC"
  max_expiries_per_bucket: 3
  delta_target: 0.25
  delta_tolerance: 0.05
  min_oi: 0
  min_quotes: 2          # filtra opciones sin bid/ask
  use_oi_weight: true
  clamp_iv: [0.0, 5.0]
  clamp_rr: [-2.0, 2.0]
  clamp_bf: [-2.0, 2.0]
```

## Ejecución manual
1. Habilitar temporalmente (`enabled: true`) y ajustar `underlying` si aplica.
2. Levantar el pipeline (alerts runner) o invocar el builder standalone:
   ```python
   import asyncio, time
   from oraculo.db import DB
   from oraculo.deribit.surface_builder import SurfaceBuilderCfg, DeribitSurfaceBuilder

   async def main():
       db = DB("${PG_DSN}")
       await db.connect()
       cfg = SurfaceBuilderCfg(enabled=True)
       builder = DeribitSurfaceBuilder(cfg)
       await builder.run_once(db, ts_now=time.time())
       await db.close()
   asyncio.run(main())
   ```

## Verificaciones
- Población de surface:
  ```sql
  SELECT count(*), max(event_time) FROM deribit.options_iv_surface;
  ```
- Vista 1m:
  ```sql
  SELECT count(*), max(bucket) FROM oraculo.iv_surface_1m WHERE underlying='BTC';
  ```
- Último bucket construido (meta):
  ```sql
  SELECT event_time, meta->>'expiry' AS expiry, tenor_bucket, moneyness_bucket
  FROM deribit.options_iv_surface
  ORDER BY event_time DESC
  LIMIT 10;
  ```

## Troubleshooting
- **Surface vacía**: aumentar `lookback_s` a 900–1200s; validar que `options_ticker` tenga datos recientes.
- **Memoria/consultas pesadas**: bajar `max_expiries_per_bucket`, restringir `lookback_s`, asegurarse de usar índices de tiempo (`options_ticker(event_time DESC)`).
- **RR/BF nulos**: puede faltar call/put 25Δ o ATM; revisar `delta_tolerance`.
- **iv_surface_1m sin refrescar**: lanzar refresco manual del cagg si existe policy:
  ```sql
  SELECT refresh_continuous_aggregate('oraculo.iv_surface_1m', now() - interval '2 hours', now());
  ```
