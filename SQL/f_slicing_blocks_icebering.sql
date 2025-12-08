SELECT *
FROM oraculo_audit.f_slicing_blocks_icebering(
    p_instrument_id => 'BINANCE:PERP:BTCUSDT',
    p_from          => '2025-12-07 14:48:46',
    p_to            => '2025-12-07 14:48:48',
    p_min_trades    => 3,
    p_min_qty_btc   => 1.0
);
