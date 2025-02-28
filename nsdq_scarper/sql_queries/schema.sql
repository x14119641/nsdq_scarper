CREATE TABLE IF NOT EXISTS tickers (
    id SERIAL PRIMARY KEY,
    ticker TEXT UNIQUE NOT NULL,
    company_name TEXT,
    stock_type TEXT,
    exchange TEXT,
    asset_class TEXT,
    is_nasdaq_listed BOOLEAN,
    is_nasdaq100 BOOLEAN,
    is_held BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_ticker_tickers ON tickers (ticker);

CREATE TABLE IF NOT EXISTS metadata (
    id SERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    exchange TEXT,
    sector TEXT,
    industry TEXT,
    one_yr_target NUMERIC(12, 2),
    today_high_low TEXT,
    share_volume BIGINT,
    average_volume BIGINT,
    previous_close NUMERIC(12,2),
    fiftytwo_week_high_low TEXT,
    market_cap BIGINT,
    pe_ratio NUMERIC(12,2),
    forward_pe_1yr  NUMERIC(12,2),
    earnings_per_share  NUMERIC(12,2),
    annualized_dividend  NUMERIC(12,2),
    ex_dividend_date DATE,
    dividend_payment_date DATE,
    yield  NUMERIC(12,2),
    special_dividend_date DATE,
    special_dividend_amount  NUMERIC(12,2),
    special_dividend_payment_date DATE,
    inserted TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ticker) REFERENCES tickers (ticker)
);

CREATE INDEX IF NOT EXISTS idx_metadata_ticker ON metadata (ticker);

CREATE TABLE IF NOT EXISTS dividends (
    id SERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    ex_date DATE,
    payment_type TEXT,
    amount NUMERIC(12, 2),
    declaration_date DATE,
    record_date DATE,
    payment_date DATE,
    currency TEXT,
    inserted TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ticker) REFERENCES tickers (ticker),
    CONSTRAINT unique_ticker_dividend UNIQUE (ticker, ex_date, payment_type, amount, declaration_date, record_date, payment_date, currency)
);

CREATE INDEX IF NOT EXISTS idx_dividends_ticker ON dividends (ticker);

CREATE TABLE IF NOT EXISTS institutional_holdings (
    id SERIAL PRIMARY KEY,
    ticker text NOT NULL,
    shares_outstanding_pct NUMERIC(12,2),
    shares_outstanding_total BIGINT,
    total_holdings_value BIGINT,
    increased_positions_holders BIGINT,
    increased_positions_shares BIGINT,
    decreased_positions_holders BIGINT,
    decreased_positions_shares BIGINT,
    held_positions_holders BIGINT,
    held_positions_shares BIGINT,
    total_positions_holders BIGINT,
    total_positions_shares BIGINT,
    new_positions_holders  BIGINT,
    new_positions_shares BIGINT,
    sold_out_positions_holders BIGINT,
    sold_out_positions_shares BIGINT,
    inserted TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ticker) REFERENCES tickers (ticker)
);

CREATE INDEX IF NOT EXISTS idx_institutional_holdings_ticker ON institutional_holdings (ticker);