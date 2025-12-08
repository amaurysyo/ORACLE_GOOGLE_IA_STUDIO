SELECT *
FROM oraculo_audit.audit_breakwall_from_raw(
  'BINANCE:PERP:BTCUSDT',
  '2025-12-05 00:00+01',
  '2025-12-05 02:00+01'
)
WHERE expected_rule IS NOT NULL;
