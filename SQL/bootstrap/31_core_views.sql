-- Generated from SQL/oraculo_schema_only.sql by tools/split_pg_dump_bootstrap.py


CREATE VIEW oraculo.oi_1m AS
 SELECT b.instrument_id,
    b.bucket,
    t.open_interest AS oi_last
   FROM (oraculo.oi_1m_base b
     JOIN LATERAL ( SELECT oi.open_interest
           FROM binance_futures.open_interest oi
          WHERE (((oi.instrument_id)::text = (b.instrument_id)::text) AND (oi.event_time = b.last_ts))
         LIMIT 1) t ON (true));

CREATE VIEW oraculo.trades_futures_1s AS
 SELECT trades_futures_1s_base.instrument_id,
    trades_futures_1s_base.bucket,
    trades_futures_1s_base.low,
    trades_futures_1s_base.high,
    trades_futures_1s_base.volume,
    trades_futures_1s_base.open,
    trades_futures_1s_base.close
   FROM oraculo.trades_futures_1s_base;

CREATE VIEW oraculo.trades_spot_1s AS
 SELECT trades_spot_1s_base.instrument_id,
    trades_spot_1s_base.bucket,
    trades_spot_1s_base.low,
    trades_spot_1s_base.high,
    trades_spot_1s_base.volume,
    trades_spot_1s_base.open,
    trades_spot_1s_base.close
   FROM oraculo.trades_spot_1s_base;

CREATE VIEW oraculo.v_alerts_recent AS
 SELECT rule_alerts.id,
    rule_alerts.instrument_id,
    rule_alerts.event_time,
    rule_alerts.rule_code,
    rule_alerts.severity,
    rule_alerts.dedup_key,
    rule_alerts.ts_first,
    rule_alerts.ts_last,
    rule_alerts.count,
    rule_alerts.context,
    rule_alerts.latency_ms,
    rule_alerts.profile,
    rule_alerts.inserted_at
   FROM oraculo.rule_alerts
  WHERE (rule_alerts.event_time > (now() - '7 days'::interval));

CREATE VIEW oraculo.v_events_throughput_1m AS
 SELECT 'futures_trades'::text AS stream,
    public.time_bucket('00:01:00'::interval, trades.event_time) AS bucket,
    count(*) AS n
   FROM binance_futures.trades
  GROUP BY (public.time_bucket('00:01:00'::interval, trades.event_time))
UNION ALL
 SELECT 'futures_depth'::text AS stream,
    public.time_bucket('00:01:00'::interval, depth.event_time) AS bucket,
    count(*) AS n
   FROM binance_futures.depth
  GROUP BY (public.time_bucket('00:01:00'::interval, depth.event_time))
UNION ALL
 SELECT 'spot_trades'::text AS stream,
    public.time_bucket('00:01:00'::interval, trades.event_time) AS bucket,
    count(*) AS n
   FROM binance_spot.trades
  GROUP BY (public.time_bucket('00:01:00'::interval, trades.event_time))
UNION ALL
 SELECT 'spot_depth'::text AS stream,
    public.time_bucket('00:01:00'::interval, depth.event_time) AS bucket,
    count(*) AS n
   FROM binance_spot.depth
  GROUP BY (public.time_bucket('00:01:00'::interval, depth.event_time))
UNION ALL
 SELECT 'deribit_trades'::text AS stream,
    public.time_bucket('00:01:00'::interval, options_trades.event_time) AS bucket,
    count(*) AS n
   FROM deribit.options_trades
  GROUP BY (public.time_bucket('00:01:00'::interval, options_trades.event_time));

CREATE VIEW oraculo.v_rule_alerts_unpacked AS
 SELECT rule_alerts.id,
    rule_alerts.instrument_id,
    rule_alerts.event_time,
    rule_alerts.rule_code,
    rule_alerts.severity,
    rule_alerts.dedup_key,
    split_part(rule_alerts.dedup_key, '|'::text, 2) AS side,
    (rule_alerts.context ->> 'type'::text) AS ev_type,
    ((rule_alerts.context ->> 'price'::text))::double precision AS price,
    ((rule_alerts.context ->> 'intensity'::text))::double precision AS intensity,
    (rule_alerts.context -> 'fields'::text) AS fields,
    rule_alerts.ts_first,
    rule_alerts.ts_last,
    rule_alerts.count,
    rule_alerts.latency_ms,
    rule_alerts.profile,
    rule_alerts.inserted_at
   FROM oraculo.rule_alerts;

CREATE VIEW oraculo.v_slice_recent AS
 SELECT slice_events.id,
    slice_events.instrument_id,
    slice_events.event_time,
    slice_events.event_type,
    slice_events.side,
    slice_events.intensity,
    slice_events.price,
    slice_events.duration_ms,
    slice_events.fields,
    slice_events.latency_ms,
    slice_events.profile,
    slice_events.inserted_at
   FROM oraculo.slice_events
  WHERE (slice_events.event_time > (now() - '7 days'::interval));
