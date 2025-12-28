-- Generated from SQL/SQL_ORACULO_BACKUP.sql by tools/split_pg_dump_bootstrap.py

CREATE VIEW oraculo.iv_surface_1m AS
 SELECT _materialized_hypertable_88.underlying,
    _materialized_hypertable_88.bucket,
    _materialized_hypertable_88.iv_avg,
    _materialized_hypertable_88.rr_25d_avg,
    _materialized_hypertable_88.bf_25d_avg
   FROM _timescaledb_internal._materialized_hypertable_88
  WHERE (_materialized_hypertable_88.bucket < COALESCE(_timescaledb_internal.to_timestamp(_timescaledb_internal.cagg_watermark(88)), '-infinity'::timestamp with time zone))
UNION ALL
 SELECT options_iv_surface.underlying,
    public.time_bucket('00:01:00'::interval, options_iv_surface.event_time) AS bucket,
    avg(options_iv_surface.iv) AS iv_avg,
    avg(options_iv_surface.rr_25d) AS rr_25d_avg,
    avg(options_iv_surface.bf_25d) AS bf_25d_avg
   FROM deribit.options_iv_surface
  WHERE (options_iv_surface.event_time >= COALESCE(_timescaledb_internal.to_timestamp(_timescaledb_internal.cagg_watermark(88)), '-infinity'::timestamp with time zone))
  GROUP BY options_iv_surface.underlying, (public.time_bucket('00:01:00'::interval, options_iv_surface.event_time));

CREATE VIEW oraculo.mark_basis_1s AS
 SELECT _materialized_hypertable_86.instrument_id,
    _materialized_hypertable_86.bucket,
    _materialized_hypertable_86.mark_price,
    _materialized_hypertable_86.index_price,
    _materialized_hypertable_86.basis_bps
   FROM _timescaledb_internal._materialized_hypertable_86
  WHERE (_materialized_hypertable_86.bucket < COALESCE(_timescaledb_internal.to_timestamp(_timescaledb_internal.cagg_watermark(86)), '-infinity'::timestamp with time zone))
UNION ALL
 SELECT mark_funding.instrument_id,
    public.time_bucket('00:00:01'::interval, mark_funding.event_time) AS bucket,
    avg(mark_funding.mark_price) AS mark_price,
    avg(mark_funding.index_price) AS index_price,
    avg(mark_funding.basis_bps) AS basis_bps
   FROM binance_futures.mark_funding
  WHERE (mark_funding.event_time >= COALESCE(_timescaledb_internal.to_timestamp(_timescaledb_internal.cagg_watermark(86)), '-infinity'::timestamp with time zone))
  GROUP BY mark_funding.instrument_id, (public.time_bucket('00:00:01'::interval, mark_funding.event_time));

CREATE VIEW oraculo.oi_1m_base AS
 SELECT _materialized_hypertable_87.instrument_id,
    _materialized_hypertable_87.bucket,
    _materialized_hypertable_87.last_ts
   FROM _timescaledb_internal._materialized_hypertable_87
  WHERE (_materialized_hypertable_87.bucket < COALESCE(_timescaledb_internal.to_timestamp(_timescaledb_internal.cagg_watermark(87)), '-infinity'::timestamp with time zone))
UNION ALL
 SELECT open_interest.instrument_id,
    public.time_bucket('00:01:00'::interval, open_interest.event_time) AS bucket,
    max(open_interest.event_time) AS last_ts
   FROM binance_futures.open_interest
  WHERE (open_interest.event_time >= COALESCE(_timescaledb_internal.to_timestamp(_timescaledb_internal.cagg_watermark(87)), '-infinity'::timestamp with time zone))
  GROUP BY open_interest.instrument_id, (public.time_bucket('00:01:00'::interval, open_interest.event_time));

CREATE VIEW oraculo.trades_futures_1s_base AS
 SELECT _materialized_hypertable_91.instrument_id,
    _materialized_hypertable_91.bucket,
    _materialized_hypertable_91.low,
    _materialized_hypertable_91.high,
    _materialized_hypertable_91.volume,
    _materialized_hypertable_91.open,
    _materialized_hypertable_91.close
   FROM _timescaledb_internal._materialized_hypertable_91
  WHERE (_materialized_hypertable_91.bucket < COALESCE(_timescaledb_internal.to_timestamp(_timescaledb_internal.cagg_watermark(91)), '-infinity'::timestamp with time zone))
UNION ALL
 SELECT trades.instrument_id,
    public.time_bucket('00:00:01'::interval, trades.event_time) AS bucket,
    min(trades.price) AS low,
    max(trades.price) AS high,
    sum(trades.qty) AS volume,
    (array_agg(trades.price ORDER BY trades.event_time, trades.trade_id_ext))[1] AS open,
    (array_agg(trades.price ORDER BY trades.event_time DESC, trades.trade_id_ext DESC))[1] AS close
   FROM binance_futures.trades
  WHERE (trades.event_time >= COALESCE(_timescaledb_internal.to_timestamp(_timescaledb_internal.cagg_watermark(91)), '-infinity'::timestamp with time zone))
  GROUP BY trades.instrument_id, (public.time_bucket('00:00:01'::interval, trades.event_time));

CREATE VIEW oraculo.trades_spot_1s_base AS
 SELECT _materialized_hypertable_92.instrument_id,
    _materialized_hypertable_92.bucket,
    _materialized_hypertable_92.low,
    _materialized_hypertable_92.high,
    _materialized_hypertable_92.volume,
    _materialized_hypertable_92.open,
    _materialized_hypertable_92.close
   FROM _timescaledb_internal._materialized_hypertable_92
  WHERE (_materialized_hypertable_92.bucket < COALESCE(_timescaledb_internal.to_timestamp(_timescaledb_internal.cagg_watermark(92)), '-infinity'::timestamp with time zone))
UNION ALL
 SELECT trades.instrument_id,
    public.time_bucket('00:00:01'::interval, trades.event_time) AS bucket,
    min(trades.price) AS low,
    max(trades.price) AS high,
    sum(trades.qty) AS volume,
    (array_agg(trades.price ORDER BY trades.event_time, trades.trade_id_ext))[1] AS open,
    (array_agg(trades.price ORDER BY trades.event_time DESC, trades.trade_id_ext DESC))[1] AS close
   FROM binance_spot.trades
  WHERE (trades.event_time >= COALESCE(_timescaledb_internal.to_timestamp(_timescaledb_internal.cagg_watermark(92)), '-infinity'::timestamp with time zone))
  GROUP BY trades.instrument_id, (public.time_bucket('00:00:01'::interval, trades.event_time));

CREATE TRIGGER ts_cagg_invalidation_trigger AFTER INSERT OR DELETE OR UPDATE ON binance_futures.mark_funding FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.continuous_agg_invalidation_trigger('50');

CREATE TRIGGER ts_cagg_invalidation_trigger AFTER INSERT OR DELETE OR UPDATE ON binance_futures.open_interest FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.continuous_agg_invalidation_trigger('51');

CREATE TRIGGER ts_cagg_invalidation_trigger AFTER INSERT OR DELETE OR UPDATE ON binance_futures.trades FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.continuous_agg_invalidation_trigger('48');

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON binance_futures.depth FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.insert_blocker();

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON binance_futures.liquidations FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.insert_blocker();

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON binance_futures.mark_funding FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.insert_blocker();

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON binance_futures.open_interest FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.insert_blocker();

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON binance_futures.top_trader_account_ratio FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.insert_blocker();

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON binance_futures.top_trader_position_ratio FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.insert_blocker();

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON binance_futures.trades FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.insert_blocker();

CREATE TRIGGER ts_cagg_invalidation_trigger AFTER INSERT OR DELETE OR UPDATE ON binance_spot.trades FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.continuous_agg_invalidation_trigger('46');

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON binance_spot.depth FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.insert_blocker();

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON binance_spot.trades FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.insert_blocker();

CREATE TRIGGER ts_cagg_invalidation_trigger AFTER INSERT OR DELETE OR UPDATE ON deribit.options_iv_surface FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.continuous_agg_invalidation_trigger('60');

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON deribit.options_book_changes FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.insert_blocker();

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON deribit.options_greeks FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.insert_blocker();

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON deribit.options_iv_surface FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.insert_blocker();

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON deribit.options_mark_price FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.insert_blocker();

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON deribit.options_signals FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.insert_blocker();

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON deribit.options_ticker FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.insert_blocker();

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON deribit.options_trades FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.insert_blocker();

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON oraculo.metrics_series FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.insert_blocker();

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON oraculo.rule_alerts FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.insert_blocker();

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON oraculo.slice_events FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.insert_blocker();

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON oraculo_audit.orderbook_snapshots FOR EACH ROW EXECUTE FUNCTION _timescaledb_internal.insert_blocker();
