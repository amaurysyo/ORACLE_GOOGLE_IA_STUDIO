-- Deduplicate rule alerts per suppress window and return id only on insert/escalation
CREATE OR REPLACE FUNCTION oraculo.upsert_rule_alert(
    p_instrument_id public.instrument_id_t,
    p_event_time timestamp with time zone,
    p_rule_code text,
    p_severity public.severity_t,
    p_dedup_key text,
    p_context jsonb,
    p_suppress_window_s integer,
    p_profile text,
    p_latency_ms integer DEFAULT NULL
) RETURNS bigint
    LANGUAGE plpgsql
AS $$
DECLARE
  v_id bigint;
  v_window_s integer;
  v_bucket_epoch double precision;
  v_ts_first timestamptz;
  v_prev_sev public.severity_t;
BEGIN
  v_window_s := CASE WHEN p_suppress_window_s IS NULL OR p_suppress_window_s <= 0 THEN 90 ELSE p_suppress_window_s END;
  v_bucket_epoch := floor(extract(epoch FROM p_event_time) / v_window_s) * v_window_s;
  v_ts_first := to_timestamp(v_bucket_epoch) AT TIME ZONE 'UTC';

  SELECT severity INTO v_prev_sev
  FROM oraculo.rule_alerts
  WHERE dedup_key = p_dedup_key AND ts_first = v_ts_first;

  INSERT INTO oraculo.rule_alerts(instrument_id,event_time,rule_code,severity,dedup_key,ts_first,ts_last,count,context,latency_ms,profile)
  VALUES (p_instrument_id,p_event_time,p_rule_code,p_severity,p_dedup_key,v_ts_first,p_event_time,1,COALESCE(p_context,'{}'::jsonb),p_latency_ms,p_profile)
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
END;
$$;
