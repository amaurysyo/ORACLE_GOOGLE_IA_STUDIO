ALTER TABLE binance_futures.mark_funding
    ADD COLUMN IF NOT EXISTS est_settle_price numeric(18,8);
