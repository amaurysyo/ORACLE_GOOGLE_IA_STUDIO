SELECT *
FROM oraculo.f_slicing_blocks(
    p_instrument_id => 'BINANCE:PERP:BTCUSDT',
    p_from          => NOW() - INTERVAL '72 hour',
    p_to            => NOW(),
    p_min_trades    => 1,
    p_min_qty_btc   => 150
);

