SELECT *
FROM oraculo_audit.audit_absorption_exact(
    'BINANCE:PERP:BTCUSDT',
    '2025-12-07 17:46:00+01',
    '2025-12-07 17:50+01',
    10.0,   -- dur_s
    1.0,  -- vol_btc
    2,      -- max_price_drift_ticks
    0.1     -- tick_size
);
