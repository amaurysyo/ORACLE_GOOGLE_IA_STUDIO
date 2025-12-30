-- Generated from SQL/oraculo_schema_only.sql by tools/split_pg_dump_bootstrap.py

SET default_table_access_method = heap;

CREATE TABLE binance_futures.mark_funding (
    instrument_id public.instrument_id_t NOT NULL,
    event_time timestamp with time zone NOT NULL,
    mark_price double precision NOT NULL,
    index_price double precision,
    funding_rate double precision,
    next_funding_time timestamp with time zone,
    basis_bps double precision,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE binance_futures.open_interest (
    instrument_id public.instrument_id_t NOT NULL,
    event_time timestamp with time zone NOT NULL,
    open_interest double precision NOT NULL,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE deribit.options_iv_surface (
    underlying text NOT NULL,
    event_time timestamp with time zone NOT NULL,
    tenor_bucket text NOT NULL,
    moneyness_bucket text NOT NULL,
    iv double precision,
    rr_25d double precision,
    bf_25d double precision,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE binance_futures.trades (
    instrument_id public.instrument_id_t NOT NULL,
    event_time timestamp with time zone NOT NULL,
    trade_id_ext bigint NOT NULL,
    price double precision NOT NULL,
    qty double precision NOT NULL,
    side public.side_t NOT NULL,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE binance_spot.trades (
    instrument_id public.instrument_id_t NOT NULL,
    event_time timestamp with time zone NOT NULL,
    trade_id_ext bigint NOT NULL,
    price double precision NOT NULL,
    qty double precision NOT NULL,
    side public.side_t NOT NULL,
    is_best_match boolean,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    buyer_order_id bigint,
    seller_order_id bigint
);

CREATE TABLE binance_futures.depth (
    instrument_id public.instrument_id_t NOT NULL,
    event_time timestamp with time zone NOT NULL,
    seq bigint NOT NULL,
    side public.side_t NOT NULL,
    action public.action_t NOT NULL,
    price double precision NOT NULL,
    qty double precision NOT NULL,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE binance_futures.top_trader_position_ratio (
    instrument_id public.instrument_id_t NOT NULL,
    event_time timestamp with time zone NOT NULL,
    long_ratio double precision,
    short_ratio double precision,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE binance_futures.top_trader_account_ratio (
    instrument_id public.instrument_id_t NOT NULL,
    event_time timestamp with time zone NOT NULL,
    long_ratio double precision,
    short_ratio double precision,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE binance_futures.liquidations (
    instrument_id public.instrument_id_t NOT NULL,
    event_time timestamp with time zone NOT NULL,
    side public.side_t NOT NULL,
    price double precision NOT NULL,
    qty double precision NOT NULL,
    quote_qty_usd double precision,
    external_id text NOT NULL,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE deribit.options_trades (
    instrument_id public.instrument_id_t NOT NULL,
    event_time timestamp with time zone NOT NULL,
    trade_id_ext text NOT NULL,
    price double precision NOT NULL,
    qty double precision NOT NULL,
    side public.side_t NOT NULL,
    underlying_price double precision,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE deribit.options_book_changes (
    instrument_id public.instrument_id_t NOT NULL,
    event_time timestamp with time zone NOT NULL,
    seq bigint NOT NULL,
    side public.side_t NOT NULL,
    action public.action_t NOT NULL,
    price double precision NOT NULL,
    qty double precision NOT NULL,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE deribit.options_ticker (
    instrument_id public.instrument_id_t NOT NULL,
    event_time timestamp with time zone NOT NULL,
    mark_iv double precision,
    delta double precision,
    gamma double precision,
    vega double precision,
    theta double precision,
    bid double precision,
    ask double precision,
    underlying_price double precision,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    open_interest numeric(20,8)
);

CREATE TABLE deribit.options_mark_price (
    instrument_id public.instrument_id_t NOT NULL,
    event_time timestamp with time zone NOT NULL,
    mark_price double precision NOT NULL,
    underlying_price double precision,
    iv_mark double precision,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE oraculo.metrics_series (
    instrument_id public.instrument_id_t NOT NULL,
    event_time timestamp with time zone NOT NULL,
    window_s integer NOT NULL,
    metric text NOT NULL,
    value double precision NOT NULL,
    profile text,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE oraculo.slice_events (
    id bigint NOT NULL,
    instrument_id public.instrument_id_t NOT NULL,
    event_time timestamp with time zone NOT NULL,
    event_type text NOT NULL,
    side public.side_t,
    intensity double precision,
    price double precision,
    duration_ms integer,
    fields jsonb DEFAULT '{}'::jsonb NOT NULL,
    latency_ms integer,
    profile text,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE oraculo.rule_alerts (
    id bigint NOT NULL,
    instrument_id public.instrument_id_t NOT NULL,
    event_time timestamp with time zone NOT NULL,
    rule_code text NOT NULL,
    severity public.severity_t NOT NULL,
    dedup_key text NOT NULL,
    ts_first timestamp with time zone NOT NULL,
    ts_last timestamp with time zone NOT NULL,
    count integer DEFAULT 1 NOT NULL,
    context jsonb DEFAULT '{}'::jsonb NOT NULL,
    latency_ms integer,
    profile text,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE oraculo_audit.orderbook_snapshots (
    instrument_id public.instrument_id_t NOT NULL,
    event_time timestamp with time zone NOT NULL,
    last_update_id bigint NOT NULL,
    best_bid double precision NOT NULL,
    best_ask double precision NOT NULL,
    spread_usd double precision NOT NULL,
    bid_prices double precision[] NOT NULL,
    bid_qtys double precision[] NOT NULL,
    ask_prices double precision[] NOT NULL,
    ask_qtys double precision[] NOT NULL,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now()
);

CREATE TABLE binance_spot.depth (
    instrument_id public.instrument_id_t NOT NULL,
    event_time timestamp with time zone NOT NULL,
    seq bigint NOT NULL,
    side public.side_t NOT NULL,
    action public.action_t NOT NULL,
    price double precision NOT NULL,
    qty double precision NOT NULL,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE deribit.options_greeks (
    instrument_id public.instrument_id_t NOT NULL,
    event_time timestamp with time zone NOT NULL,
    delta double precision,
    gamma double precision,
    vega double precision,
    theta double precision,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE deribit.options_instruments (
    instrument_id public.instrument_id_t NOT NULL,
    underlying text NOT NULL,
    expiry date NOT NULL,
    strike double precision NOT NULL,
    option_type text NOT NULL,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT options_instruments_option_type_check CHECK ((option_type = ANY (ARRAY['C'::text, 'P'::text])))
);

CREATE TABLE deribit.options_signals (
    underlying text NOT NULL,
    event_time timestamp with time zone NOT NULL,
    signal_type text NOT NULL,
    intensity double precision,
    context jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE oraculo.alert_dispatch_log (
    id bigint NOT NULL,
    alert_id bigint NOT NULL,
    alert_ts_first timestamp with time zone NOT NULL,
    event_time timestamp with time zone DEFAULT now() NOT NULL,
    target text NOT NULL,
    status text NOT NULL,
    error text,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    channel text DEFAULT 'rules'::text NOT NULL,
    text text DEFAULT ''::text NOT NULL,
    extra jsonb DEFAULT '{}'::jsonb NOT NULL
);

CREATE SEQUENCE oraculo.alert_dispatch_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE oraculo.alert_dispatch_log_id_seq OWNED BY oraculo.alert_dispatch_log.id;

CREATE TABLE oraculo.instrument_catalog (
    instrument_id public.instrument_id_t NOT NULL,
    exchange text NOT NULL,
    market_type text NOT NULL,
    symbol text NOT NULL,
    underlying text,
    expiry date,
    strike double precision,
    option_type text,
    tick_size double precision,
    lot_size double precision,
    active boolean DEFAULT true NOT NULL,
    meta jsonb DEFAULT '{}'::jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT instrument_catalog_option_type_check CHECK (((option_type = ANY (ARRAY['C'::text, 'P'::text])) OR (option_type IS NULL)))
);

CREATE SEQUENCE oraculo.rule_alerts_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE oraculo.rule_alerts_id_seq OWNED BY oraculo.rule_alerts.id;

CREATE TABLE oraculo.rule_telemetry (
    ts_bucket timestamp with time zone NOT NULL,
    instrument_id text NOT NULL,
    profile text NOT NULL,
    rule text NOT NULL,
    side text NOT NULL,
    emitted integer DEFAULT 0 NOT NULL,
    disc_abs_no_best integer DEFAULT 0 NOT NULL,
    disc_bw_basis integer DEFAULT 0 NOT NULL,
    disc_bw_dep_refill integer DEFAULT 0 NOT NULL,
    disc_dom_spread integer DEFAULT 0 NOT NULL,
    disc_metrics_none integer DEFAULT 0 NOT NULL,
    disc_iv_missing integer DEFAULT 0 NOT NULL,
    disc_oi_missing integer DEFAULT 0 NOT NULL,
    disc_oi_low integer DEFAULT 0 NOT NULL,
    disc_basis_vel_low integer DEFAULT 0 NOT NULL,
    disc_dep_low integer DEFAULT 0 NOT NULL,
    disc_refill_high integer DEFAULT 0 NOT NULL,
    disc_top_levels_gate integer DEFAULT 0 NOT NULL
);

CREATE SEQUENCE oraculo.slice_events_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE oraculo.slice_events_id_seq OWNED BY oraculo.slice_events.id;

CREATE TABLE oraculo.telegram_bots (
    name text NOT NULL,
    token text NOT NULL,
    chat_id text NOT NULL,
    rate_limit_per_min integer DEFAULT 60 NOT NULL,
    active boolean DEFAULT true NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE oraculo_bt.bt_equity (
    run_id bigint NOT NULL,
    bucket timestamp with time zone NOT NULL,
    equity double precision NOT NULL
);

CREATE TABLE oraculo_bt.bt_metrics (
    run_id bigint NOT NULL,
    metric text NOT NULL,
    value double precision NOT NULL
);

CREATE TABLE oraculo_bt.bt_runs (
    run_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    instrument_id text NOT NULL,
    venue text NOT NULL,
    tf_s integer NOT NULL,
    t_start timestamp with time zone NOT NULL,
    t_end timestamp with time zone NOT NULL,
    strategy_code text NOT NULL,
    params jsonb NOT NULL,
    notes text,
    CONSTRAINT bt_runs_tf_s_check CHECK ((tf_s = ANY (ARRAY[30, 60, 120, 300]))),
    CONSTRAINT bt_runs_venue_check CHECK ((venue = ANY (ARRAY['SPOT'::text, 'FUTURES'::text])))
);

CREATE SEQUENCE oraculo_bt.bt_runs_run_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE oraculo_bt.bt_runs_run_id_seq OWNED BY oraculo_bt.bt_runs.run_id;

CREATE TABLE oraculo_bt.bt_trades (
    run_id bigint NOT NULL,
    ts_entry timestamp with time zone NOT NULL,
    ts_exit timestamp with time zone,
    side text NOT NULL,
    px_entry double precision NOT NULL,
    px_exit double precision,
    qty double precision DEFAULT 1 NOT NULL,
    pnl double precision,
    CONSTRAINT bt_trades_side_check CHECK ((side = ANY (ARRAY['LONG'::text, 'SHORT'::text])))
);

ALTER TABLE ONLY oraculo.alert_dispatch_log ALTER COLUMN id SET DEFAULT nextval('oraculo.alert_dispatch_log_id_seq'::regclass);

ALTER TABLE ONLY oraculo.rule_alerts ALTER COLUMN id SET DEFAULT nextval('oraculo.rule_alerts_id_seq'::regclass);

ALTER TABLE ONLY oraculo.slice_events ALTER COLUMN id SET DEFAULT nextval('oraculo.slice_events_id_seq'::regclass);

ALTER TABLE ONLY oraculo_bt.bt_runs ALTER COLUMN run_id SET DEFAULT nextval('oraculo_bt.bt_runs_run_id_seq'::regclass);

ALTER TABLE ONLY binance_futures.depth
    ADD CONSTRAINT depth_pkey PRIMARY KEY (instrument_id, event_time, seq, side, price);

ALTER TABLE ONLY binance_futures.liquidations
    ADD CONSTRAINT liquidations_pkey PRIMARY KEY (instrument_id, event_time, external_id);

ALTER TABLE ONLY binance_futures.mark_funding
    ADD CONSTRAINT mark_funding_pkey PRIMARY KEY (instrument_id, event_time);

ALTER TABLE ONLY binance_futures.open_interest
    ADD CONSTRAINT open_interest_pkey PRIMARY KEY (instrument_id, event_time);

ALTER TABLE ONLY binance_futures.top_trader_account_ratio
    ADD CONSTRAINT top_trader_account_ratio_pkey PRIMARY KEY (instrument_id, event_time);

ALTER TABLE ONLY binance_futures.top_trader_position_ratio
    ADD CONSTRAINT top_trader_position_ratio_pkey PRIMARY KEY (instrument_id, event_time);

ALTER TABLE ONLY binance_futures.trades
    ADD CONSTRAINT trades_pkey PRIMARY KEY (instrument_id, event_time, trade_id_ext);

ALTER TABLE ONLY binance_spot.depth
    ADD CONSTRAINT depth_pkey PRIMARY KEY (instrument_id, event_time, seq, side, price);

ALTER TABLE ONLY binance_spot.trades
    ADD CONSTRAINT trades_pkey PRIMARY KEY (instrument_id, event_time, trade_id_ext);

ALTER TABLE ONLY deribit.options_book_changes
    ADD CONSTRAINT options_book_changes_pkey PRIMARY KEY (instrument_id, event_time, seq, side, price);

ALTER TABLE ONLY deribit.options_greeks
    ADD CONSTRAINT options_greeks_pkey PRIMARY KEY (instrument_id, event_time);

ALTER TABLE ONLY deribit.options_instruments
    ADD CONSTRAINT options_instruments_pkey PRIMARY KEY (instrument_id);

ALTER TABLE ONLY deribit.options_iv_surface
    ADD CONSTRAINT options_iv_surface_pkey PRIMARY KEY (underlying, event_time, tenor_bucket, moneyness_bucket);

ALTER TABLE ONLY deribit.options_mark_price
    ADD CONSTRAINT options_mark_price_pkey PRIMARY KEY (instrument_id, event_time);

ALTER TABLE ONLY deribit.options_signals
    ADD CONSTRAINT options_signals_pkey PRIMARY KEY (underlying, event_time, signal_type);

ALTER TABLE ONLY deribit.options_ticker
    ADD CONSTRAINT options_ticker_pkey PRIMARY KEY (instrument_id, event_time);

ALTER TABLE ONLY deribit.options_trades
    ADD CONSTRAINT options_trades_pkey PRIMARY KEY (instrument_id, event_time, trade_id_ext);

ALTER TABLE ONLY oraculo.alert_dispatch_log
    ADD CONSTRAINT alert_dispatch_log_pkey PRIMARY KEY (id);

ALTER TABLE ONLY oraculo.instrument_catalog
    ADD CONSTRAINT instrument_catalog_pkey PRIMARY KEY (instrument_id);

ALTER TABLE ONLY oraculo.metrics_series
    ADD CONSTRAINT metrics_series_pkey PRIMARY KEY (instrument_id, event_time, metric, window_s);

ALTER TABLE ONLY oraculo.rule_alerts
    ADD CONSTRAINT rule_alerts_dedup_key_ts_first_key UNIQUE (dedup_key, ts_first);

ALTER TABLE ONLY oraculo.rule_alerts
    ADD CONSTRAINT rule_alerts_pkey PRIMARY KEY (id, ts_first);

ALTER TABLE ONLY oraculo.rule_telemetry
    ADD CONSTRAINT rule_telemetry_pkey PRIMARY KEY (ts_bucket, instrument_id, profile, rule, side);

ALTER TABLE ONLY oraculo.slice_events
    ADD CONSTRAINT slice_events_pkey PRIMARY KEY (id, event_time);

ALTER TABLE ONLY oraculo.telegram_bots
    ADD CONSTRAINT telegram_bots_pkey PRIMARY KEY (name);

ALTER TABLE ONLY oraculo_audit.orderbook_snapshots
    ADD CONSTRAINT orderbook_snapshots_pkey PRIMARY KEY (instrument_id, event_time);

ALTER TABLE ONLY oraculo_bt.bt_equity
    ADD CONSTRAINT bt_equity_pk PRIMARY KEY (run_id, bucket);

ALTER TABLE ONLY oraculo_bt.bt_metrics
    ADD CONSTRAINT bt_metrics_pk PRIMARY KEY (run_id, metric);

ALTER TABLE ONLY oraculo_bt.bt_runs
    ADD CONSTRAINT bt_runs_pkey PRIMARY KEY (run_id);

ALTER TABLE ONLY oraculo_bt.bt_trades
    ADD CONSTRAINT bt_trades_pk PRIMARY KEY (run_id, ts_entry);

CREATE INDEX depth_event_time_idx ON binance_futures.depth USING btree (event_time DESC);

CREATE INDEX depth_instrument_id_event_time_idx ON binance_futures.depth USING btree (instrument_id, event_time DESC);

CREATE INDEX idx_bfut_depth_inst_time_desc ON binance_futures.depth USING btree (instrument_id, event_time DESC);

CREATE INDEX idx_bfut_depth_side_price ON binance_futures.depth USING btree (side, price);

CREATE INDEX idx_bfut_mark_inst_time_desc ON binance_futures.mark_funding USING btree (instrument_id, event_time DESC);

CREATE INDEX idx_bfut_mark_time ON binance_futures.mark_funding USING btree (event_time DESC);

CREATE INDEX idx_bfut_oi_inst_time_desc ON binance_futures.open_interest USING btree (instrument_id, event_time DESC);

CREATE INDEX idx_bfut_oi_time ON binance_futures.open_interest USING btree (event_time DESC);

CREATE INDEX idx_bfut_trades_inst_time_desc ON binance_futures.trades USING btree (instrument_id, event_time DESC);

CREATE INDEX idx_bfut_trades_time ON binance_futures.trades USING btree (event_time DESC);

CREATE INDEX liquidations_event_time_idx ON binance_futures.liquidations USING btree (event_time DESC);

CREATE INDEX liquidations_instrument_id_event_time_idx ON binance_futures.liquidations USING btree (instrument_id, event_time DESC);

CREATE INDEX mark_funding_event_time_idx ON binance_futures.mark_funding USING btree (event_time DESC);

CREATE INDEX open_interest_event_time_idx ON binance_futures.open_interest USING btree (event_time DESC);

CREATE INDEX top_trader_account_ratio_event_time_idx ON binance_futures.top_trader_account_ratio USING btree (event_time DESC);

CREATE INDEX top_trader_position_ratio_event_time_idx ON binance_futures.top_trader_position_ratio USING btree (event_time DESC);

CREATE INDEX trades_event_time_idx ON binance_futures.trades USING btree (event_time DESC);

CREATE INDEX trades_instrument_id_event_time_idx ON binance_futures.trades USING btree (instrument_id, event_time DESC);

CREATE INDEX depth_event_time_idx ON binance_spot.depth USING btree (event_time DESC);

CREATE INDEX depth_instrument_id_event_time_idx ON binance_spot.depth USING btree (instrument_id, event_time DESC);

CREATE INDEX idx_bspot_depth_inst_time_desc ON binance_spot.depth USING btree (instrument_id, event_time DESC);

CREATE INDEX idx_bspot_depth_side_price ON binance_spot.depth USING btree (side, price);

CREATE INDEX idx_bspot_trades_inst_time_desc ON binance_spot.trades USING btree (instrument_id, event_time DESC);

CREATE INDEX idx_bspot_trades_time ON binance_spot.trades USING btree (event_time DESC);

CREATE INDEX ix_bspot_trades_buyer_order_id ON binance_spot.trades USING btree (buyer_order_id) WHERE (buyer_order_id IS NOT NULL);

CREATE INDEX ix_bspot_trades_seller_order_id ON binance_spot.trades USING btree (seller_order_id) WHERE (seller_order_id IS NOT NULL);

CREATE INDEX trades_event_time_idx ON binance_spot.trades USING btree (event_time DESC);

CREATE INDEX trades_instrument_id_event_time_idx ON binance_spot.trades USING btree (instrument_id, event_time DESC);

CREATE INDEX idx_deriv_greeks_time ON deribit.options_greeks USING btree (event_time DESC);

CREATE INDEX idx_deriv_ivsurf_under_time_desc ON deribit.options_iv_surface USING btree (underlying, event_time DESC);

CREATE INDEX idx_deriv_opt_ticker_inst_time_desc ON deribit.options_ticker USING btree (instrument_id, event_time DESC);

CREATE INDEX idx_deriv_opt_trades_inst_time_desc ON deribit.options_trades USING btree (instrument_id, event_time DESC);

CREATE INDEX idx_deriv_ticker_time ON deribit.options_ticker USING btree (event_time DESC);

CREATE INDEX options_book_changes_event_time_idx ON deribit.options_book_changes USING btree (event_time DESC);

CREATE INDEX options_book_changes_instrument_id_event_time_idx ON deribit.options_book_changes USING btree (instrument_id, event_time DESC);

CREATE INDEX options_greeks_event_time_idx ON deribit.options_greeks USING btree (event_time DESC);

CREATE INDEX options_iv_surface_event_time_idx ON deribit.options_iv_surface USING btree (event_time DESC);

CREATE INDEX options_mark_price_event_time_idx ON deribit.options_mark_price USING btree (event_time DESC);

CREATE INDEX options_signals_event_time_idx ON deribit.options_signals USING btree (event_time DESC);

CREATE INDEX options_ticker_event_time_idx ON deribit.options_ticker USING btree (event_time DESC);

CREATE INDEX options_trades_event_time_idx ON deribit.options_trades USING btree (event_time DESC);

CREATE INDEX options_trades_instrument_id_event_time_idx ON deribit.options_trades USING btree (instrument_id, event_time DESC);

CREATE INDEX alert_dispatch_log_alert_idx ON oraculo.alert_dispatch_log USING btree (alert_id);

CREATE INDEX alert_dispatch_log_ts_first_idx ON oraculo.alert_dispatch_log USING btree (alert_ts_first);

CREATE INDEX idx_alerts_inst_tsfirst_desc ON oraculo.rule_alerts USING btree (instrument_id, ts_first DESC);

CREATE INDEX idx_alerts_rule_sev ON oraculo.rule_alerts USING btree (rule_code, severity);

CREATE INDEX idx_instr_by_underlying ON oraculo.instrument_catalog USING btree (underlying, expiry, strike);

CREATE INDEX idx_metrics_inst_time_desc ON oraculo.metrics_series USING btree (instrument_id, event_time DESC);

CREATE INDEX idx_rule_alerts_id ON oraculo.rule_alerts USING btree (id);

CREATE INDEX idx_slice_by_id ON oraculo.slice_events USING btree (id);

CREATE INDEX idx_slice_by_time ON oraculo.slice_events USING btree (event_time DESC);

CREATE INDEX idx_slice_by_type ON oraculo.slice_events USING btree (event_type, side);

CREATE INDEX metrics_series_event_time_idx ON oraculo.metrics_series USING btree (event_time DESC);

CREATE INDEX metrics_series_instrument_id_event_time_idx ON oraculo.metrics_series USING btree (instrument_id, event_time DESC);

CREATE INDEX rule_alerts_ts_first_idx ON oraculo.rule_alerts USING btree (ts_first DESC);

CREATE INDEX orderbook_snapshots_event_time_idx ON oraculo_audit.orderbook_snapshots USING btree (event_time DESC);

ALTER TABLE ONLY oraculo_bt.bt_equity
    ADD CONSTRAINT bt_equity_run_id_fkey FOREIGN KEY (run_id) REFERENCES oraculo_bt.bt_runs(run_id) ON DELETE CASCADE;

ALTER TABLE ONLY oraculo_bt.bt_metrics
    ADD CONSTRAINT bt_metrics_run_id_fkey FOREIGN KEY (run_id) REFERENCES oraculo_bt.bt_runs(run_id) ON DELETE CASCADE;

ALTER TABLE ONLY oraculo_bt.bt_trades
    ADD CONSTRAINT bt_trades_run_id_fkey FOREIGN KEY (run_id) REFERENCES oraculo_bt.bt_runs(run_id) ON DELETE CASCADE;


-- Completed on 2025-12-28 17:58:16

--
-- PostgreSQL database dump complete
