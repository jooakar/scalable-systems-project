CREATE TABLE IF NOT EXISTS crossover_events (
    window_end TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    ema38 DOUBLE PRECISION NOT NULL,
    ema100 DOUBLE PRECISION NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, window_end)
);

SELECT create_hypertable(
    'crossover_events',
    by_range('window_end'),
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS crossover_events_window_end_idx
    ON crossover_events (window_end DESC);

CREATE INDEX IF NOT EXISTS crossover_events_symbol_window_end_idx
    ON crossover_events (symbol, window_end DESC);

CREATE OR REPLACE VIEW latest_crossover_events AS
SELECT DISTINCT ON (symbol)
    symbol,
    window_end,
    signal_type,
    ema38,
    ema100,
    received_at
FROM crossover_events
ORDER BY symbol, window_end DESC;
