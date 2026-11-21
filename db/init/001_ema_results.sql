CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS ema_results (
    window_end TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    last_price DOUBLE PRECISION NOT NULL,
    ema38 DOUBLE PRECISION NOT NULL,
    ema100 DOUBLE PRECISION NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, window_end)
);

SELECT create_hypertable(
    'ema_results',
    by_range('window_end'),
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS ema_results_window_end_idx
    ON ema_results (window_end DESC);

CREATE INDEX IF NOT EXISTS ema_results_symbol_window_end_idx
    ON ema_results (symbol, window_end DESC);

CREATE OR REPLACE VIEW latest_ema_results AS
SELECT DISTINCT ON (symbol)
    symbol,
    window_end,
    last_price,
    ema38,
    ema100,
    received_at
FROM ema_results
ORDER BY symbol, window_end DESC;
