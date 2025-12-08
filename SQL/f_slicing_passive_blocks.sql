SELECT *
FROM oraculo.f_slicing_passive_blocks(
    p_instrument_id => 'BINANCE:PERP:BTCUSDT',
    p_from          => NOW() - INTERVAL '24 hours',
    p_to            => NOW(),
    p_gap_ms        => 120,   -- mismo que en el detector
    p_min_inserts   => 6,     -- mismo k_min que SlicingPassConfig.k_min
    p_min_qty_btc   => 5.0    -- mismo qty_min que SlicingPassConfig.qty_min
)
ORDER BY t_start
LIMIT 100;
