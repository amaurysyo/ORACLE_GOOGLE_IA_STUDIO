-- Generated from SQL/oraculo_schema_only.sql by tools/split_pg_dump_bootstrap.py


CREATE FUNCTION oraculo.f_slicing_blocks(p_instrument_id public.instrument_id_t, p_from timestamp with time zone, p_to timestamp with time zone, p_min_trades integer DEFAULT 1, p_min_qty_btc double precision DEFAULT 0) RETURNS TABLE(instrument_id public.instrument_id_t, block_id bigint, side public.side_t, price double precision, t_start timestamp with time zone, t_end timestamp with time zone, n_trades bigint, qty_btc double precision, duration_s double precision, qty_min double precision, qty_max double precision, qty_stddev double precision, qty_all_equal boolean, qty_almost_equal boolean, pattern text)
    LANGUAGE sql
    AS '
WITH t AS (
    SELECT
        instrument_id,
        event_time,
        side,
        price,
        qty,
        LAG(side)  OVER (PARTITION BY instrument_id ORDER BY event_time) AS prev_side,
        LAG(price) OVER (PARTITION BY instrument_id ORDER BY event_time) AS prev_price
    FROM binance_futures.trades
    WHERE instrument_id = p_instrument_id
      AND event_time BETWEEN p_from AND p_to
),
marked AS (
    SELECT
        instrument_id,
        event_time,
        side,
        price,
        qty,
        CASE
            WHEN prev_side IS NULL
              OR side  <> prev_side
              OR price <> prev_price
            THEN 1 ELSE 0
        END AS new_block
    FROM t
),
blocks AS (
    SELECT
        instrument_id,
        event_time,
        side,
        price,
        qty,
        SUM(new_block) OVER (
            PARTITION BY instrument_id
            ORDER BY event_time
            ROWS UNBOUNDED PRECEDING
        )::bigint AS block_id
    FROM marked
),
agg AS (
    SELECT
        instrument_id,
        block_id,
        side,
        price,
        MIN(event_time) AS t_start,
        MAX(event_time) AS t_end,
        COUNT(*)        AS n_trades,
        SUM(qty)        AS qty_btc,
        EXTRACT(EPOCH FROM (MAX(event_time) - MIN(event_time))) AS duration_s,
        MIN(qty)        AS qty_min,
        MAX(qty)        AS qty_max,
        stddev_pop(qty) AS qty_stddev,
        (MIN(qty) = MAX(qty) AND COUNT(*) > 1) AS qty_all_equal
    FROM blocks
    GROUP BY instrument_id, block_id, side, price
    HAVING
        COUNT(*)    >= p_min_trades
        AND SUM(qty) >= p_min_qty_btc
)
SELECT
    instrument_id,
    block_id,
    side,
    price,
    t_start,
    t_end,
    n_trades,
    qty_btc,
    duration_s,
    qty_min,
    qty_max,
    qty_stddev,
    qty_all_equal,
    (qty_stddev IS NOT NULL AND qty_stddev < 0.01) AS qty_almost_equal,
    CASE
        WHEN qty_all_equal
          OR (qty_stddev IS NOT NULL AND qty_stddev < 0.01)
        THEN ''iceberg_like''
        ELSE ''mixed_hitting''
    END AS pattern
FROM agg
ORDER BY t_start;
';

CREATE FUNCTION oraculo.f_slicing_passive_blocks(p_instrument_id public.instrument_id_t, p_from timestamp with time zone, p_to timestamp with time zone, p_gap_ms integer DEFAULT 120, p_min_inserts integer DEFAULT 1, p_min_qty_btc double precision DEFAULT 0) RETURNS TABLE(instrument_id public.instrument_id_t, block_id bigint, side public.side_t, price double precision, t_start timestamp with time zone, t_end timestamp with time zone, n_inserts bigint, qty_btc double precision, duration_s double precision, qty_min double precision, qty_max double precision, qty_stddev double precision, qty_all_equal boolean, qty_almost_equal boolean, pattern text)
    LANGUAGE sql
    AS '
WITH depth_ins AS (
    SELECT
        instrument_id,
        event_time,
        side,
        price,
        qty,
        LAG(event_time) OVER (PARTITION BY instrument_id ORDER BY event_time) AS prev_ts,
        LAG(side)       OVER (PARTITION BY instrument_id ORDER BY event_time) AS prev_side,
        LAG(price)      OVER (PARTITION BY instrument_id ORDER BY event_time) AS prev_price
    FROM binance_futures.depth
    WHERE instrument_id = p_instrument_id
      AND event_time BETWEEN p_from AND p_to
      AND action = ''update''
      AND qty > 0
),
marked AS (
    SELECT
        instrument_id,
        event_time,
        side,
        price,
        qty,
        CASE
            WHEN prev_ts IS NULL
              OR event_time - prev_ts > (p_gap_ms * INTERVAL ''1 millisecond'')
              OR side  <> prev_side
              OR price <> prev_price
            THEN 1 ELSE 0
        END AS new_block
    FROM depth_ins
),
blocks AS (
    SELECT
        instrument_id,
        event_time,
        side,
        price,
        qty,
        SUM(new_block) OVER (
            PARTITION BY instrument_id
            ORDER BY event_time
            ROWS UNBOUNDED PRECEDING
        )::bigint AS block_id
    FROM marked
),
agg AS (
    SELECT
        instrument_id,
        block_id,
        side,
        price,
        MIN(event_time) AS t_start,
        MAX(event_time) AS t_end,
        COUNT(*)        AS n_inserts,
        SUM(qty)        AS qty_btc,
        EXTRACT(EPOCH FROM (MAX(event_time) - MIN(event_time))) AS duration_s,
        MIN(qty)        AS qty_min,
        MAX(qty)        AS qty_max,
        stddev_pop(qty) AS qty_stddev,
        (MIN(qty) = MAX(qty) AND COUNT(*) > 1) AS qty_all_equal
    FROM blocks
    GROUP BY instrument_id, block_id, side, price
    HAVING
        COUNT(*)    >= p_min_inserts
        AND SUM(qty) >= p_min_qty_btc
)
SELECT
    instrument_id,
    block_id,
    side,
    price,
    t_start,
    t_end,
    n_inserts,
    qty_btc,
    duration_s,
    qty_min,
    qty_max,
    qty_stddev,
    qty_all_equal,
    (qty_stddev IS NOT NULL AND qty_stddev < 0.01) AS qty_almost_equal,
    CASE
        WHEN qty_all_equal
          OR (qty_stddev IS NOT NULL AND qty_stddev < 0.01)
        THEN ''passive_uniform''
        ELSE ''passive_mixed''
    END AS pattern
FROM agg
ORDER BY t_start;
';

CREATE FUNCTION oraculo.insert_depth_futures(p_instrument_id public.instrument_id_t, p_event_time timestamp with time zone, p_seq bigint, p_side public.side_t, p_action public.action_t, p_price double precision, p_qty double precision, p_meta jsonb) RETURNS void
    LANGUAGE plpgsql
    AS '
BEGIN
  INSERT INTO binance_futures.depth(instrument_id,event_time,seq,side,action,price,qty,meta)
  VALUES (p_instrument_id,p_event_time,p_seq,p_side,p_action,p_price,p_qty,COALESCE(p_meta,''{}''::jsonb))
  ON CONFLICT DO NOTHING;
END ';

CREATE FUNCTION oraculo.touch_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS '
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END ';

CREATE FUNCTION oraculo.upsert_rule_alert(p_instrument_id public.instrument_id_t, p_event_time timestamp with time zone, p_rule_code text, p_severity public.severity_t, p_dedup_key text, p_context jsonb, p_suppress_window_s integer, p_profile text, p_latency_ms integer DEFAULT NULL) RETURNS bigint
    LANGUAGE plpgsql
    AS '
DECLARE
  v_id bigint;
  v_window_s integer;
  v_bucket_epoch double precision;
  v_ts_first timestamptz;
  v_prev_sev public.severity_t;
BEGIN
  v_window_s := CASE WHEN p_suppress_window_s IS NULL OR p_suppress_window_s <= 0 THEN 90 ELSE p_suppress_window_s END;
  v_bucket_epoch := floor(extract(epoch FROM p_event_time) / v_window_s) * v_window_s;
  v_ts_first := to_timestamp(v_bucket_epoch) AT TIME ZONE ''UTC'';

  SELECT severity INTO v_prev_sev
  FROM oraculo.rule_alerts
  WHERE dedup_key = p_dedup_key AND ts_first = v_ts_first;

  INSERT INTO oraculo.rule_alerts(instrument_id,event_time,rule_code,severity,dedup_key,ts_first,ts_last,count,context,latency_ms,profile)
  VALUES (p_instrument_id,p_event_time,p_rule_code,p_severity,p_dedup_key,v_ts_first,p_event_time,1,COALESCE(p_context,''{}''::jsonb),p_latency_ms,p_profile)
  ON CONFLICT (dedup_key, ts_first) DO UPDATE
    SET ts_last = GREATEST(oraculo.rule_alerts.ts_last, EXCLUDED.ts_last),
        count   = oraculo.rule_alerts.count + 1,
        severity = GREATEST(oraculo.rule_alerts.severity, EXCLUDED.severity),
        context = oraculo.rule_alerts.context || EXCLUDED.context,
        event_time = EXCLUDED.event_time,
        latency_ms = EXCLUDED.latency_ms
  RETURNING id INTO v_id;

  IF v_prev_sev IS NULL THEN
    RETURN v_id;
  END IF;
  IF p_severity > v_prev_sev THEN
    RETURN v_id;
  END IF;
  RETURN NULL;
END ';

CREATE FUNCTION oraculo_audit.audit_absorption_exact(p_instrument_id public.instrument_id_t, p_from timestamp with time zone, p_to timestamp with time zone, p_vol_btc double precision, p_dur_s double precision, p_max_drift_ticks integer, p_tick_size double precision) RETURNS TABLE(event_time timestamp with time zone, side public.side_t, price double precision, vol_btc double precision, drift_ticks double precision)
    LANGUAGE plpgsql
    AS '
DECLARE
    -- Libro temporal (best)
    v_best_bid  double precision;
    v_best_ask  double precision;

    -- Anclas (equivalentes a _best_anchor["sell"] / ["buy"])
    v_anchor_ask_for_buy  double precision;  -- ancla ASK para agresiones BUY
    v_anchor_bid_for_sell double precision;  -- ancla BID para agresiones SELL

    -- Flags de "hemos visto este lado en la ventana"
    v_had_buy  boolean := false;   -- _had_side_in_window["buy"]
    v_had_sell boolean := false;   -- _had_side_in_window["sell"]

    -- Ventana deslizante
    v_cutoff_ts  timestamptz;
    v_sum_buy    double precision;
    v_sum_sell   double precision;

    -- Flags/auxiliares para reset
    v_changed   boolean;
    v_has_buy   boolean;
    v_has_sell  boolean;

    -- Drift
    v_drift_ok     boolean;
    v_drift_ticks  double precision;
    v_tick_size    double precision;

    r record;
BEGIN
    -- tick_size efectivo (como en Python: cfg.tick_size or 1.0)
    IF p_tick_size IS NULL OR p_tick_size <= 0 THEN
        v_tick_size := 1.0;
    ELSE
        v_tick_size := p_tick_size;
    END IF;

    -- =========================
    -- 1) TABLAS TEMPORALES
    -- =========================

    DROP TABLE IF EXISTS oraculo_audit_tmp_book;
    DROP TABLE IF EXISTS oraculo_audit_tmp_win;

    CREATE TEMP TABLE oraculo_audit_tmp_book (
        side  side_t,
        price double precision,
        qty   double precision,
        CONSTRAINT oraculo_audit_tmp_book_pk PRIMARY KEY (side, price)
    ) ON COMMIT DROP;

    CREATE TEMP TABLE oraculo_audit_tmp_win (
        ts    timestamptz,
        side  side_t,
        price double precision,
        qty   double precision
    ) ON COMMIT DROP;

    -- =========================
    -- 2) RECORRIDO DEL STREAM
    --    depth + trades mezclados
    -- =========================
    FOR r IN
        SELECT
            s.event_time,
            s.kind,
            s.side,
            s.action,
            s.price,
            s.qty
        FROM (
            -- Depth events
            SELECT
                d.event_time,
                ''depth''::text     AS kind,
                d.side,
                d.action          AS action,
                d.price,
                d.qty
            FROM binance_futures.depth d
            WHERE d.instrument_id = p_instrument_id
              AND d.event_time >= (p_from - make_interval(secs => p_dur_s))
              AND d.event_time <= p_to

            UNION ALL

            -- Trades (agresiones)
            SELECT
                t.event_time,
                ''trade''::text     AS kind,
                t.side,
                NULL::action_t    AS action,
                t.price,
                t.qty
            FROM binance_futures.trades t
            WHERE t.instrument_id = p_instrument_id
              AND t.event_time >= (p_from - make_interval(secs => p_dur_s))
              AND t.event_time <= p_to
        ) AS s
        -- IMPORTANTE: depth antes que trade en mismo timestamp
        ORDER BY
            s.event_time,
            (s.kind = ''trade'')::int  -- 0 depth, 1 trade
    LOOP
        -- ======================================
        -- A) EVENTOS DE DEPTH -> actualizar book
        -- ======================================
        IF r.kind = ''depth'' THEN
            IF r.action IN (''insert''::action_t, ''update''::action_t) THEN
                IF r.qty <= 0 THEN
                    DELETE FROM oraculo_audit_tmp_book b
                    WHERE b.side = r.side
                      AND b.price = r.price;
                ELSE
                    INSERT INTO oraculo_audit_tmp_book(side, price, qty)
                    VALUES (r.side, r.price, r.qty)
                    ON CONFLICT ON CONSTRAINT oraculo_audit_tmp_book_pk DO UPDATE
                    SET qty = EXCLUDED.qty;
                END IF;
            ELSIF r.action = ''delete''::action_t THEN
                DELETE FROM oraculo_audit_tmp_book b
                WHERE b.side = r.side
                  AND b.price = r.price;
            END IF;

        -- ======================================
        -- B) EVENTOS DE TRADE -> AbsorptionDetector
        -- ======================================
        ELSE
            -- Emulación de engine.book.best()
            SELECT max(b.price)
            INTO v_best_bid
            FROM oraculo_audit_tmp_book b
            WHERE b.side = ''buy''::side_t
              AND b.qty > 0;

            SELECT min(b.price)
            INTO v_best_ask
            FROM oraculo_audit_tmp_book b
            WHERE b.side = ''sell''::side_t
              AND b.qty > 0;

            -- ===== AbsorptionDetector.on_trade() =====

            -- insertar trade en ventana
            INSERT INTO oraculo_audit_tmp_win(ts, side, price, qty)
            VALUES (r.event_time, r.side, r.price, r.qty);

            -- _reset_if_expired(ts)
            v_cutoff_ts := r.event_time - make_interval(secs => p_dur_s);

            SELECT EXISTS(
                       SELECT 1
                       FROM oraculo_audit_tmp_win w
                       WHERE w.ts < v_cutoff_ts
                   )
            INTO v_changed;

            IF v_changed THEN
                DELETE FROM oraculo_audit_tmp_win w
                WHERE w.ts < v_cutoff_ts;

                -- Recalcular si quedan BUY / SELL en ventana
                SELECT COALESCE(EXISTS(
                           SELECT 1
                           FROM oraculo_audit_tmp_win w
                           WHERE w.side = ''buy''::side_t
                       ), false)
                INTO v_has_buy;

                IF NOT v_has_buy THEN
                    v_had_buy := false;
                    v_anchor_ask_for_buy := NULL;
                END IF;

                SELECT COALESCE(EXISTS(
                           SELECT 1
                           FROM oraculo_audit_tmp_win w
                           WHERE w.side = ''sell''::side_t
                       ), false)
                INTO v_has_sell;

                IF NOT v_has_sell THEN
                    v_had_sell := false;
                    v_anchor_bid_for_sell := NULL;
                END IF;
            END IF;

            -- _maybe_set_anchor(side)
            IF r.side = ''buy''::side_t THEN
                -- Primer BUY en ventana desde el último reset → ancla ASK actual
                IF NOT v_had_buy THEN
                    v_anchor_ask_for_buy := v_best_ask;
                    v_had_buy := true;
                END IF;
            ELSE
                -- Primer SELL en ventana desde el último reset → ancla BID actual
                IF NOT v_had_sell THEN
                    v_anchor_bid_for_sell := v_best_bid;
                    v_had_sell := true;
                END IF;
            END IF;

            -- acumula volumen por lado en la ventana
            SELECT COALESCE(sum(w.qty), 0.0)
            INTO v_sum_buy
            FROM oraculo_audit_tmp_win w
            WHERE w.side = ''buy''::side_t;

            SELECT COALESCE(sum(w.qty), 0.0)
            INTO v_sum_sell
            FROM oraculo_audit_tmp_win w
            WHERE w.side = ''sell''::side_t;

            -- ==========================
            -- Chequeo BUY (absorción en ASK)
            -- ==========================
            v_drift_ok := false;
            v_drift_ticks := NULL;

            IF v_sum_buy >= p_vol_btc THEN
                IF v_best_ask IS NOT NULL AND v_anchor_ask_for_buy IS NOT NULL THEN
                    v_drift_ticks := ABS(v_best_ask - v_anchor_ask_for_buy) / v_tick_size;
                    IF v_drift_ticks <= p_max_drift_ticks THEN
                        v_drift_ok := true;
                    END IF;
                END IF;

                IF v_drift_ok AND r.event_time >= p_from THEN
                    event_time  := r.event_time;
                    side        := ''buy''::side_t;
                    price       := r.price;
                    vol_btc     := v_sum_buy;
                    drift_ticks := v_drift_ticks;
                    RETURN NEXT;
                END IF;
            END IF;

            -- ==========================
            -- Chequeo SELL (absorción en BID)
            -- ==========================
            v_drift_ok := false;
            v_drift_ticks := NULL;

            IF v_sum_sell >= p_vol_btc THEN
                IF v_best_bid IS NOT NULL AND v_anchor_bid_for_sell IS NOT NULL THEN
                    v_drift_ticks := ABS(v_best_bid - v_anchor_bid_for_sell) / v_tick_size;
                    IF v_drift_ticks <= p_max_drift_ticks THEN
                        v_drift_ok := true;
                    END IF;
                END IF;

                IF v_drift_ok AND r.event_time >= p_from THEN
                    event_time  := r.event_time;
                    side        := ''sell''::side_t;
                    price       := r.price;
                    vol_btc     := v_sum_sell;
                    drift_ticks := v_drift_ticks;
                    RETURN NEXT;
                END IF;
            END IF;
        END IF;
    END LOOP;

    RETURN;
END;
';

CREATE FUNCTION oraculo_audit.audit_basis_vel_from_mark(p_instrument_id text, p_start timestamp with time zone, p_end timestamp with time zone) RETURNS TABLE(event_time timestamp with time zone, basis_bps double precision, basis_vel_bps_s double precision)
    LANGUAGE sql
    AS '
WITH marks AS (
  SELECT
    event_time,
    COALESCE(
      basis_bps,
      CASE
        WHEN index_price IS NOT NULL AND index_price <> 0
        THEN (mark_price / index_price - 1.0) * 10000.0
        ELSE NULL
      END
    ) AS basis_bps
  FROM binance_futures.mark_funding
  WHERE instrument_id = p_instrument_id
    AND event_time BETWEEN p_start - interval ''10 minutes'' AND p_end
  ORDER BY event_time
),
vel AS (
  SELECT
    event_time,
    basis_bps,
    LAG(basis_bps)  OVER (ORDER BY event_time) AS prev_bps,
    LAG(event_time) OVER (ORDER BY event_time) AS prev_ts
  FROM marks
)
SELECT
  event_time,
  basis_bps,
  CASE
    WHEN prev_bps IS NULL OR prev_ts IS NULL THEN NULL
    ELSE
      (basis_bps - prev_bps)
      / GREATEST(
          EXTRACT(EPOCH FROM (event_time - prev_ts)),
          1e-6
        )
  END AS basis_vel_bps_s
FROM vel
ORDER BY event_time;
';

CREATE FUNCTION oraculo_audit.audit_breakwall_candidates(p_instrument_id text, p_start timestamp with time zone, p_end timestamp with time zone, p_n_min integer DEFAULT 3, p_basis_abs_min double precision DEFAULT 1.5, p_dep_min double precision DEFAULT 0.40, p_refill_max double precision DEFAULT 0.60) RETURNS TABLE(ts_sec timestamp with time zone, side text, k_slices bigint, qty_slices numeric, basis_vel_bps_s double precision, dep_bid double precision, dep_ask double precision, refill_bid_3s double precision, refill_ask_3s double precision, expected_rule text)
    LANGUAGE sql
    AS '
WITH slices AS (
    SELECT
        date_trunc(''second'', event_time) AS ts_sec,
        side,
        count(*)::bigint                AS k_slices,
        sum(intensity)::numeric         AS qty_slices
    FROM oraculo.slice_events
    WHERE instrument_id = p_instrument_id
      AND event_type   = ''slicing_aggr''
      AND event_time BETWEEN p_start AND p_end
    GROUP BY 1,2
),
metrics AS (
    SELECT
        date_trunc(''second'', event_time) AS ts_sec,
        metric,
        avg(value)::double precision    AS value
    FROM oraculo.metrics_series
    WHERE instrument_id = p_instrument_id
      AND event_time BETWEEN p_start AND p_end
      AND metric IN (''basis_vel_bps_s'',''dep_bid'',''dep_ask'',
                     ''refill_bid_3s'',''refill_ask_3s'')
    GROUP BY 1,2
),
snap AS (
    SELECT
        ts_sec,
        max(CASE WHEN metric=''basis_vel_bps_s'' THEN value END) AS basis_vel_bps_s,
        max(CASE WHEN metric=''dep_bid''        THEN value END) AS dep_bid,
        max(CASE WHEN metric=''dep_ask''        THEN value END) AS dep_ask,
        max(CASE WHEN metric=''refill_bid_3s''  THEN value END) AS refill_bid_3s,
        max(CASE WHEN metric=''refill_ask_3s''  THEN value END) AS refill_ask_3s
    FROM metrics
    GROUP BY ts_sec
)
SELECT
    s.ts_sec,
    s.side,
    s.k_slices,
    s.qty_slices,
    snap.basis_vel_bps_s,
    snap.dep_bid,
    snap.dep_ask,
    snap.refill_bid_3s,
    snap.refill_ask_3s,
    CASE
      WHEN snap.basis_vel_bps_s >= p_basis_abs_min
           AND snap.dep_ask      >= p_dep_min
           AND snap.refill_ask_3s < p_refill_max
           AND s.side = ''buy''
           AND s.k_slices >= p_n_min
        THEN ''R1''
      WHEN snap.basis_vel_bps_s <= -p_basis_abs_min
           AND snap.dep_bid      >= p_dep_min
           AND snap.refill_bid_3s < p_refill_max
           AND s.side = ''sell''
           AND s.k_slices >= p_n_min
        THEN ''R2''
      ELSE NULL
    END AS expected_rule
FROM slices s
JOIN snap
  ON snap.ts_sec = s.ts_sec
ORDER BY s.ts_sec, s.side;
';

CREATE FUNCTION oraculo_audit.audit_breakwall_from_raw(p_instrument_id text, p_start timestamp with time zone, p_end timestamp with time zone, p_min_slices integer DEFAULT 3, p_basis_abs_min double precision DEFAULT 1.5, p_dep_min double precision DEFAULT 0.40, p_refill_max double precision DEFAULT 0.60) RETURNS TABLE(ts_sec timestamp with time zone, side text, k_slices bigint, qty_slices numeric, basis_vel_bps_s double precision, dep_bid double precision, dep_ask double precision, refill_bid_3s double precision, refill_ask_3s double precision, expected_rule text)
    LANGUAGE sql
    AS '
WITH snap AS (
  SELECT *
  FROM oraculo_audit.audit_breakwall_snapshots_from_raw(
    p_instrument_id, p_start, p_end, 1
  )
)
SELECT
  ts_sec,
  side,
  k_slices,
  qty_slices,
  basis_vel_bps_s,
  dep_bid,
  dep_ask,
  refill_bid_3s,
  refill_ask_3s,
  CASE
    WHEN basis_vel_bps_s IS NULL THEN NULL
    WHEN side = ''buy''
         AND k_slices >= p_min_slices
         AND basis_vel_bps_s >= p_basis_abs_min
         AND dep_ask        >= p_dep_min
         AND refill_ask_3s  <  p_refill_max
      THEN ''R1''
    WHEN side = ''sell''
         AND k_slices >= p_min_slices
         AND basis_vel_bps_s <= -p_basis_abs_min
         AND dep_bid        >= p_dep_min
         AND refill_bid_3s  <  p_refill_max
      THEN ''R2''
    ELSE NULL
  END AS expected_rule
FROM snap
ORDER BY ts_sec, side;
';

CREATE FUNCTION oraculo_audit.audit_breakwall_from_raw_vs_alerts(p_instrument_id text, p_start timestamp with time zone, p_end timestamp with time zone, p_n_min integer DEFAULT 3, p_dep_pct double precision DEFAULT 0.40, p_basis_vel_abs_bps_s double precision DEFAULT 1.5, p_refill_min_pct double precision DEFAULT 0.60, p_window_s double precision DEFAULT 3.0, p_require_depletion boolean DEFAULT true, p_tolerance interval DEFAULT '00:00:01'::interval) RETURNS TABLE(ts_sec timestamp with time zone, expected_rule text, side text, k_slices bigint, qty_slices numeric, basis_vel_bps_s double precision, alert_id bigint, alert_time timestamp with time zone, alert_severity text)
    LANGUAGE sql
    AS '
WITH candidatos AS (
  SELECT *
  FROM oraculo_audit.audit_breakwall_from_raw(
    p_instrument_id,
    p_start,
    p_end,
    p_n_min,
    p_dep_pct,
    p_basis_vel_abs_bps_s,
    p_refill_min_pct,
    p_window_s,
    p_require_depletion
  )
  WHERE expected_rule IS NOT NULL
),
alerts AS (
  SELECT
    id,
    rule_code,
    event_time,
    severity
  FROM oraculo.rule_alerts
  WHERE instrument_id = p_instrument_id
    AND rule_code IN (''R1'',''R2'')
    AND event_time BETWEEN p_start AND p_end
)
SELECT
  c.ts_sec,
  c.expected_rule,
  c.side,
  c.k_slices,
  c.qty_slices,
  c.basis_vel_bps_s,
  a.id         AS alert_id,
  a.event_time AS alert_time,
  a.severity   AS alert_severity
FROM candidatos c
LEFT JOIN alerts a
  ON a.rule_code = c.expected_rule
 AND a.event_time BETWEEN c.ts_sec - p_tolerance
                       AND c.ts_sec + p_tolerance
ORDER BY c.ts_sec, c.side;
';

CREATE FUNCTION oraculo_audit.audit_breakwall_metrics_for_slicing(p_instrument_id text, p_start timestamp with time zone, p_end timestamp with time zone, p_min_slices integer DEFAULT 1) RETURNS TABLE(ts_sec timestamp with time zone, side text, k_slices bigint, qty_slices numeric, basis_vel_bps_s double precision, dep_bid double precision, dep_ask double precision, refill_bid_3s double precision, refill_ask_3s double precision)
    LANGUAGE sql
    AS '
WITH slices AS (
    SELECT *
    FROM oraculo_audit.audit_slicing_by_second(
        p_instrument_id,
        p_start,
        p_end,
        p_min_slices
    )
),
metrics AS (
    SELECT
        date_trunc(''second'', event_time) AS ts_sec,
        metric,
        avg(value)::double precision    AS value
    FROM oraculo.metrics_series
    WHERE instrument_id = p_instrument_id
      AND event_time BETWEEN p_start AND p_end
      AND metric IN (
          ''basis_vel_bps_s'',
          ''dep_bid'',''dep_ask'',
          ''refill_bid_3s'',''refill_ask_3s''
      )
    GROUP BY 1,2
),
snap AS (
    SELECT
        ts_sec,
        max(CASE WHEN metric=''basis_vel_bps_s'' THEN value END) AS basis_vel_bps_s,
        max(CASE WHEN metric=''dep_bid''        THEN value END) AS dep_bid,
        max(CASE WHEN metric=''dep_ask''        THEN value END) AS dep_ask,
        max(CASE WHEN metric=''refill_bid_3s''  THEN value END) AS refill_bid_3s,
        max(CASE WHEN metric=''refill_ask_3s''  THEN value END) AS refill_ask_3s
    FROM metrics
    GROUP BY ts_sec
)
SELECT
    s.ts_sec,
    s.side,
    s.k_slices,
    s.qty_slices,
    snap.basis_vel_bps_s,
    snap.dep_bid,
    snap.dep_ask,
    snap.refill_bid_3s,
    snap.refill_ask_3s
FROM slices s
LEFT JOIN snap
  ON snap.ts_sec = s.ts_sec
ORDER BY s.ts_sec, s.side;
';

CREATE FUNCTION oraculo_audit.audit_breakwall_snapshots_from_raw(p_instrument_id text, p_start timestamp with time zone, p_end timestamp with time zone, p_min_slices integer DEFAULT 1) RETURNS TABLE(ts_sec timestamp with time zone, side text, k_slices bigint, qty_slices numeric, basis_bps double precision, basis_vel_bps_s double precision, dep_bid double precision, dep_ask double precision, refill_bid_3s double precision, refill_ask_3s double precision)
    LANGUAGE sql
    AS '
WITH slices AS (
  SELECT *
  FROM oraculo_audit.audit_slicing_by_second(
    p_instrument_id, p_start, p_end, p_min_slices
  )
),
basis_series AS (
  SELECT *
  FROM oraculo_audit.audit_basis_vel_from_mark(
    p_instrument_id, p_start, p_end
  )
),
-- 1) depth con prev_qty por (instrument_id, side, price)
depth_delta AS (
  SELECT
    instrument_id,
    event_time,
    side,
    price,
    qty,
    COALESCE(
      LAG(qty) OVER (
        PARTITION BY instrument_id, side, price
        ORDER BY event_time, seq
      ),
      0.0
    ) AS prev_qty
  FROM binance_futures.depth
  WHERE instrument_id = p_instrument_id
    AND event_time BETWEEN p_start - interval ''3 seconds'' AND p_end
),
-- 2) eventos de inserción/borrado lógicos (deltas)
depth_events AS (
  SELECT
    event_time,
    side,
    GREATEST(qty - prev_qty, 0.0) AS ins,
    GREATEST(prev_qty - qty, 0.0) AS dels
  FROM depth_delta
)
SELECT
  s.ts_sec,
  s.side,
  s.k_slices,
  s.qty_slices,
  b.basis_bps,
  b.basis_vel_bps_s,
  -- dep/refill en ventana (ts_sec-3s, ts_sec]
  CASE WHEN ins_buy + del_buy > 0
       THEN del_buy / (ins_buy + del_buy)
       ELSE 0 END AS dep_bid,
  CASE WHEN ins_sell + del_sell > 0
       THEN del_sell / (ins_sell + del_sell)
       ELSE 0 END AS dep_ask,
  CASE WHEN del_buy > 0
       THEN LEAST(ins_buy / del_buy, 1.0)
       ELSE 0 END AS refill_bid_3s,
  CASE WHEN del_sell > 0
       THEN LEAST(ins_sell / del_sell, 1.0)
       ELSE 0 END AS refill_ask_3s
FROM slices s
-- basis/basis_vel: último mark_funding <= ts_sec
LEFT JOIN LATERAL (
  SELECT
    event_time,
    basis_bps,
    basis_vel_bps_s
  FROM basis_series m
  WHERE m.event_time <= s.ts_sec
  ORDER BY m.event_time DESC
  LIMIT 1
) b ON true
-- suma ins/dels de los últimos 3s por lado
LEFT JOIN LATERAL (
  SELECT
    COALESCE(SUM(CASE WHEN side = ''buy''  THEN ins  END), 0.0) AS ins_buy,
    COALESCE(SUM(CASE WHEN side = ''buy''  THEN dels END), 0.0) AS del_buy,
    COALESCE(SUM(CASE WHEN side = ''sell'' THEN ins  END), 0.0) AS ins_sell,
    COALESCE(SUM(CASE WHEN side = ''sell'' THEN dels END), 0.0) AS del_sell
  FROM depth_events de
  WHERE de.event_time >  s.ts_sec - interval ''3 seconds''
    AND de.event_time <= s.ts_sec
) w ON true
ORDER BY s.ts_sec, s.side;
';

CREATE FUNCTION oraculo_audit.audit_slicing_by_second(p_instrument_id text, p_start timestamp with time zone, p_end timestamp with time zone, p_min_slices integer DEFAULT 1) RETURNS TABLE(ts_sec timestamp with time zone, side text, k_slices bigint, qty_slices numeric)
    LANGUAGE sql
    AS '
SELECT
    date_trunc(''second'', event_time)      AS ts_sec,
    side,
    count(*)::bigint                     AS k_slices,
    sum(intensity)::numeric              AS qty_slices
FROM oraculo.slice_events
WHERE instrument_id = p_instrument_id
  AND event_type    = ''slicing_aggr''
  AND event_time BETWEEN p_start AND p_end
GROUP BY 1,2
HAVING count(*) >= p_min_slices
ORDER BY ts_sec, side;
';

CREATE FUNCTION oraculo_audit.f_replay_depth_book(p_instrument_id public.instrument_id_t, p_from timestamp with time zone, p_to timestamp with time zone, p_depth_levels integer DEFAULT 1000) RETURNS TABLE(event_time timestamp with time zone, best_bid double precision, best_ask double precision, spread_usd double precision, nz_bids integer, nz_asks integer, dom_bid double precision, dom_ask double precision)
    LANGUAGE plpgsql
    AS '
DECLARE
    rec          binance_futures.depth%ROWTYPE;
    v_last_ts    timestamptz;
    v_best_bid   double precision;
    v_best_ask   double precision;
    v_nz_bids    integer;
    v_nz_asks    integer;
    v_total      integer;
BEGIN
    -- Estado temporal del libro: 1 fila por lado+precio con la qty actual
    CREATE TEMP TABLE IF NOT EXISTS tmp_book_state (
        side  side_t,
        price double precision,
        qty   double precision,
        PRIMARY KEY (side, price)
    ) ON COMMIT DROP;

    -- Muy importante: limpiar entre llamadas en la misma sesión
    TRUNCATE TABLE tmp_book_state;

    v_last_ts := NULL;

    FOR rec IN
        SELECT d.*
        FROM binance_futures.depth AS d
        WHERE d.instrument_id = p_instrument_id
          AND d.event_time >= p_from
          AND d.event_time <  p_to
        ORDER BY d.event_time, d.seq, d.side, d.price
    LOOP
        -- Cuando cambia el timestamp, emitimos snapshot del libro anterior
        IF v_last_ts IS NULL THEN
            v_last_ts := rec.event_time;
        ELSIF rec.event_time <> v_last_ts THEN
            -- calcular best bid/ask y dominancia sobre tmp_book_state
            SELECT
                max(price) FILTER (WHERE side = ''buy''  AND qty > 0) AS best_bid,
                min(price) FILTER (WHERE side = ''sell'' AND qty > 0) AS best_ask
            INTO v_best_bid, v_best_ask
            FROM tmp_book_state;

            IF v_best_bid IS NOT NULL AND v_best_ask IS NOT NULL THEN
                -- contar niveles no vacíos en top N
                SELECT count(*) INTO v_nz_bids
                FROM (
                    SELECT 1
                    FROM tmp_book_state
                    WHERE side=''buy'' AND qty > 0
                    ORDER BY price DESC
                    LIMIT p_depth_levels
                ) q;

                SELECT count(*) INTO v_nz_asks
                FROM (
                    SELECT 1
                    FROM tmp_book_state
                    WHERE side=''sell'' AND qty > 0
                    ORDER BY price ASC
                    LIMIT p_depth_levels
                ) q;

                v_total := COALESCE(v_nz_bids,0) + COALESCE(v_nz_asks,0);

                -- Asignamos a las columnas de salida
                event_time := v_last_ts;
                best_bid   := v_best_bid;
                best_ask   := v_best_ask;
                spread_usd := v_best_ask - v_best_bid;
                nz_bids    := v_nz_bids;
                nz_asks    := v_nz_asks;
                dom_bid    := CASE WHEN v_total > 0 THEN v_nz_bids::double precision / v_total ELSE NULL END;
                dom_ask    := CASE WHEN v_total > 0 THEN v_nz_asks::double precision / v_total ELSE NULL END;

                RETURN NEXT;
            END IF;

            v_last_ts := rec.event_time;
        END IF;

        -- Aplicar la actualización al estado del libro
        IF rec.action = ''delete'' OR rec.qty = 0.0 THEN
            DELETE FROM tmp_book_state
            WHERE side = rec.side AND price = rec.price;
        ELSE
            INSERT INTO tmp_book_state(side, price, qty)
            VALUES (rec.side, rec.price, rec.qty)
            ON CONFLICT (side, price) DO
            UPDATE SET qty = EXCLUDED.qty;
        END IF;
    END LOOP;

    -- Snapshot final para el último timestamp del bucle
    IF v_last_ts IS NOT NULL THEN
        SELECT
            max(price) FILTER (WHERE side = ''buy''  AND qty > 0) AS best_bid,
            min(price) FILTER (WHERE side = ''sell'' AND qty > 0) AS best_ask
        INTO v_best_bid, v_best_ask
        FROM tmp_book_state;

        IF v_best_bid IS NOT NULL AND v_best_ask IS NOT NULL THEN
            SELECT count(*) INTO v_nz_bids
            FROM (
                SELECT 1
                FROM tmp_book_state
                WHERE side=''buy'' AND qty > 0
                ORDER BY price DESC
                LIMIT p_depth_levels
            ) q;

            SELECT count(*) INTO v_nz_asks
            FROM (
                SELECT 1
                FROM tmp_book_state
                WHERE side=''sell'' AND qty > 0
                ORDER BY price ASC
                LIMIT p_depth_levels
            ) q;

            v_total := COALESCE(v_nz_bids,0) + COALESCE(v_nz_asks,0);

            event_time := v_last_ts;
            best_bid   := v_best_bid;
            best_ask   := v_best_ask;
            spread_usd := v_best_ask - v_best_bid;
            nz_bids    := v_nz_bids;
            nz_asks    := v_nz_asks;
            dom_bid    := CASE WHEN v_total > 0 THEN v_nz_bids::double precision / v_total ELSE NULL END;
            dom_ask    := CASE WHEN v_total > 0 THEN v_nz_asks::double precision / v_total ELSE NULL END;

            RETURN NEXT;
        END IF;
    END IF;

    RETURN;
END;
';

CREATE FUNCTION oraculo_audit.f_slicing_blocks(p_instrument_id public.instrument_id_t, p_from timestamp with time zone, p_to timestamp with time zone, p_min_trades integer DEFAULT 1, p_min_qty_btc double precision DEFAULT 0) RETURNS TABLE(instrument_id public.instrument_id_t, block_id bigint, side public.side_t, price double precision, t_start timestamp with time zone, t_end timestamp with time zone, n_trades bigint, qty_btc double precision, duration_s double precision, qty_min double precision, qty_max double precision, qty_stddev double precision, qty_all_equal boolean, qty_almost_equal boolean, pattern text)
    LANGUAGE sql
    AS '
WITH t AS (
    SELECT
        instrument_id,
        event_time,
        side,
        price,
        qty,
        LAG(side)  OVER (PARTITION BY instrument_id ORDER BY event_time) AS prev_side,
        LAG(price) OVER (PARTITION BY instrument_id ORDER BY event_time) AS prev_price
    FROM binance_futures.trades
    WHERE instrument_id = p_instrument_id
      AND event_time BETWEEN p_from AND p_to
),
marked AS (
    SELECT
        instrument_id,
        event_time,
        side,
        price,
        qty,
        CASE
            WHEN prev_side IS NULL
              OR side  <> prev_side
              OR price <> prev_price
            THEN 1 ELSE 0
        END AS new_block
    FROM t
),
blocks AS (
    SELECT
        instrument_id,
        event_time,
        side,
        price,
        qty,
        SUM(new_block) OVER (
            PARTITION BY instrument_id
            ORDER BY event_time
            ROWS UNBOUNDED PRECEDING
        )::bigint AS block_id
    FROM marked
),
agg AS (
    SELECT
        instrument_id,
        block_id,
        side,
        price,
        MIN(event_time) AS t_start,
        MAX(event_time) AS t_end,
        COUNT(*)        AS n_trades,
        SUM(qty)        AS qty_btc,
        EXTRACT(EPOCH FROM (MAX(event_time) - MIN(event_time))) AS duration_s,
        MIN(qty)        AS qty_min,
        MAX(qty)        AS qty_max,
        stddev_pop(qty) AS qty_stddev,
        (MIN(qty) = MAX(qty) AND COUNT(*) > 1) AS qty_all_equal
    FROM blocks
    GROUP BY instrument_id, block_id, side, price
    HAVING
        COUNT(*)    >= p_min_trades
        AND SUM(qty) >= p_min_qty_btc
)
SELECT
    instrument_id,
    block_id,
    side,
    price,
    t_start,
    t_end,
    n_trades,
    qty_btc,
    duration_s,
    qty_min,
    qty_max,
    qty_stddev,
    qty_all_equal,
    (qty_stddev IS NOT NULL AND qty_stddev < 0.01) AS qty_almost_equal,
    CASE
        WHEN qty_all_equal
          OR (qty_stddev IS NOT NULL AND qty_stddev < 0.01)
        THEN ''iceberg_like''
        ELSE ''mixed_hitting''
    END AS pattern
FROM agg
ORDER BY t_start;
';

CREATE FUNCTION oraculo_audit.f_slicing_blocks_icebering(p_instrument_id public.instrument_id_t, p_from timestamp with time zone, p_to timestamp with time zone, p_min_trades integer DEFAULT 1, p_min_qty_btc double precision DEFAULT 0) RETURNS TABLE(instrument_id public.instrument_id_t, block_id bigint, side public.side_t, price double precision, t_start timestamp with time zone, t_end timestamp with time zone, n_trades bigint, qty_btc double precision, duration_s double precision, qty_min double precision, qty_max double precision, qty_stddev double precision, qty_all_equal boolean, qty_almost_equal boolean, pattern text)
    LANGUAGE sql
    AS '
WITH t AS (
    SELECT
        instrument_id,
        event_time,
        side,
        price,
        qty,
        LAG(side)  OVER (PARTITION BY instrument_id ORDER BY event_time) AS prev_side,
        LAG(price) OVER (PARTITION BY instrument_id ORDER BY event_time) AS prev_price,
        LAG(qty)   OVER (PARTITION BY instrument_id ORDER BY event_time) AS prev_qty
    FROM binance_futures.trades
    WHERE instrument_id = p_instrument_id
      AND event_time BETWEEN p_from AND p_to
),
marked AS (
    SELECT
        instrument_id,
        event_time,
        side,
        price,
        qty,
        CASE
            WHEN prev_side IS NULL
              OR side  <> prev_side
              OR price <> prev_price
              OR qty   <> prev_qty        -- 👈 NUEVO: cortamos bloque si cambia qty
            THEN 1 ELSE 0
        END AS new_block
    FROM t
),
blocks AS (
    SELECT
        instrument_id,
        event_time,
        side,
        price,
        qty,
        SUM(new_block) OVER (
            PARTITION BY instrument_id
            ORDER BY event_time
            ROWS UNBOUNDED PRECEDING
        )::bigint AS block_id
    FROM marked
),
agg AS (
    SELECT
        instrument_id,
        block_id,
        side,
        price,
        MIN(event_time) AS t_start,
        MAX(event_time) AS t_end,
        COUNT(*)        AS n_trades,
        SUM(qty)        AS qty_btc,
        EXTRACT(EPOCH FROM (MAX(event_time) - MIN(event_time))) AS duration_s,
        MIN(qty)        AS qty_min,
        MAX(qty)        AS qty_max,
        stddev_pop(qty) AS qty_stddev,
        (MIN(qty) = MAX(qty) AND COUNT(*) > 1) AS qty_all_equal
    FROM blocks
    GROUP BY instrument_id, block_id, side, price
    HAVING
        COUNT(*)    >= p_min_trades
        AND SUM(qty) >= p_min_qty_btc
)
SELECT
    instrument_id,
    block_id,
    side,
    price,
    t_start,
    t_end,
    n_trades,
    qty_btc,
    duration_s,
    qty_min,
    qty_max,
    qty_stddev,
    qty_all_equal,
    (qty_stddev IS NOT NULL AND qty_stddev < 0.01) AS qty_almost_equal,
    CASE
        WHEN qty_all_equal
          OR (qty_stddev IS NOT NULL AND qty_stddev < 0.01)
        THEN ''iceberg_like''
        ELSE ''mixed_hitting''
    END AS pattern
FROM agg
ORDER BY t_start;
';

CREATE FUNCTION oraculo_audit.f_slicing_passive_blocks(p_instrument_id public.instrument_id_t, p_from timestamp with time zone, p_to timestamp with time zone, p_gap_ms integer DEFAULT 120, p_min_inserts integer DEFAULT 1, p_min_qty_btc double precision DEFAULT 0) RETURNS TABLE(instrument_id public.instrument_id_t, block_id bigint, side public.side_t, price double precision, t_start timestamp with time zone, t_end timestamp with time zone, n_inserts bigint, qty_btc double precision, duration_s double precision, qty_min double precision, qty_max double precision, qty_stddev double precision, qty_all_equal boolean, qty_almost_equal boolean, pattern text)
    LANGUAGE sql
    AS '
WITH depth_ins AS (
    SELECT
        instrument_id,
        event_time,
        side,
        price,
        qty,
        LAG(event_time) OVER (PARTITION BY instrument_id ORDER BY event_time) AS prev_ts,
        LAG(side)       OVER (PARTITION BY instrument_id ORDER BY event_time) AS prev_side,
        LAG(price)      OVER (PARTITION BY instrument_id ORDER BY event_time) AS prev_price
    FROM binance_futures.depth
    WHERE instrument_id = p_instrument_id
      AND event_time BETWEEN p_from AND p_to
      AND action = ''update''
      AND qty > 0
),
marked AS (
    SELECT
        instrument_id,
        event_time,
        side,
        price,
        qty,
        CASE
            WHEN prev_ts IS NULL
              OR event_time - prev_ts > (p_gap_ms * INTERVAL ''1 millisecond'')
              OR side  <> prev_side
              OR price <> prev_price
            THEN 1 ELSE 0
        END AS new_block
    FROM depth_ins
),
blocks AS (
    SELECT
        instrument_id,
        event_time,
        side,
        price,
        qty,
        SUM(new_block) OVER (
            PARTITION BY instrument_id
            ORDER BY event_time
            ROWS UNBOUNDED PRECEDING
        )::bigint AS block_id
    FROM marked
),
agg AS (
    SELECT
        instrument_id,
        block_id,
        side,
        price,
        MIN(event_time) AS t_start,
        MAX(event_time) AS t_end,
        COUNT(*)        AS n_inserts,
        SUM(qty)        AS qty_btc,
        EXTRACT(EPOCH FROM (MAX(event_time) - MIN(event_time))) AS duration_s,
        MIN(qty)        AS qty_min,
        MAX(qty)        AS qty_max,
        stddev_pop(qty) AS qty_stddev,
        (MIN(qty) = MAX(qty) AND COUNT(*) > 1) AS qty_all_equal
    FROM blocks
    GROUP BY instrument_id, block_id, side, price
    HAVING
        COUNT(*)    >= p_min_inserts
        AND SUM(qty) >= p_min_qty_btc
)
SELECT
    instrument_id,
    block_id,
    side,
    price,
    t_start,
    t_end,
    n_inserts,
    qty_btc,
    duration_s,
    qty_min,
    qty_max,
    qty_stddev,
    qty_all_equal,
    (qty_stddev IS NOT NULL AND qty_stddev < 0.01) AS qty_almost_equal,
    CASE
        WHEN qty_all_equal
          OR (qty_stddev IS NOT NULL AND qty_stddev < 0.01)
        THEN ''passive_uniform''
        ELSE ''passive_mixed''
    END AS pattern
FROM agg
ORDER BY t_start;
';

CREATE FUNCTION oraculo_bt.mk_candles_futures(p_instrument_id text, p_tf_s integer, p_start timestamp with time zone, p_end timestamp with time zone) RETURNS TABLE(bucket timestamp with time zone, open double precision, high double precision, low double precision, close double precision, volume double precision)
    LANGUAGE sql
    AS '
SELECT
  time_bucket((p_tf_s || '' seconds'')::interval, bucket) AS tb,
  (ARRAY_AGG("open"  ORDER BY bucket ASC ))[1]  AS "open",
  MAX("high") AS "high",
  MIN("low")  AS "low",
  (ARRAY_AGG("close" ORDER BY bucket DESC))[1]  AS "close",
  SUM(volume) AS volume
FROM oraculo.trades_futures_1s_base
WHERE instrument_id = p_instrument_id
  AND bucket >= p_start AND bucket < p_end
GROUP BY tb
ORDER BY tb;
';

CREATE FUNCTION oraculo_bt.mk_candles_spot(p_instrument_id text, p_tf_s integer, p_start timestamp with time zone, p_end timestamp with time zone) RETURNS TABLE(bucket timestamp with time zone, open double precision, high double precision, low double precision, close double precision, volume double precision)
    LANGUAGE sql
    AS '
SELECT
  time_bucket((p_tf_s || '' seconds'')::interval, bucket) AS tb,
  (ARRAY_AGG("open"  ORDER BY bucket ASC ))[1]  AS "open",
  MAX("high") AS "high",
  MIN("low")  AS "low",
  (ARRAY_AGG("close" ORDER BY bucket DESC))[1]  AS "close",
  SUM(volume) AS volume
FROM oraculo.trades_spot_1s_base
WHERE instrument_id = p_instrument_id
  AND bucket >= p_start AND bucket < p_end
GROUP BY tb
ORDER BY tb;
';

CREATE TRIGGER trg_opt_instr_touch BEFORE UPDATE ON deribit.options_instruments FOR EACH ROW EXECUTE FUNCTION oraculo.touch_updated_at();

CREATE TRIGGER trg_instr_touch BEFORE UPDATE ON oraculo.instrument_catalog FOR EACH ROW EXECUTE FUNCTION oraculo.touch_updated_at();
