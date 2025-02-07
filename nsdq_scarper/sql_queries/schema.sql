CREATE TABLE IF NOT EXISTS tickers (
    id SERIAL PRIMARY KEY,
    ticker TEXT UNIQUE NOT NULL,
    companyName TEXT,
    stockType TEXT,
    exchange TEXT,
    assetClass TEXT,
    isNasdaqListed BOOLEAN,
    isNasdaq100 BOOLEAN,
    isHeld BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_ticker_tickers ON tickers (ticker);

CREATE TABLE IF NOT EXISTS metadata (
    id SERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    exchange TEXT,
    sector TEXT,
    industry TEXT,
    oneYrTarget NUMERIC(12, 2),
    todayHighLow TEXT,
    shareVolume BIGINT,
    averageVolume BIGINT,
    previousClose NUMERIC(12,2),
    fiftTwoWeekHighLow TEXT,
    marketCap BIGINT,
    PERatio NUMERIC(12,2),
    forwardPE1Yr  NUMERIC(12,2),
    earningsPerShare  NUMERIC(12,2),
    annualizedDividend  NUMERIC(12,2),
    exDividendDate DATE,
    dividendPaymentDate DATE,
    yield  NUMERIC(12,2),
    specialDividendDate DATE,
    specialDividendAmount  NUMERIC(12,2),
    specialDividendPaymentDate DATE,
    inserted TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ticker) REFERENCES tickers (ticker)
);

CREATE INDEX IF NOT EXISTS idx_metadata_ticker ON metadata (ticker);

CREATE TABLE IF NOT EXISTS dividends (
    id SERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    exOrEffDate DATE,
    paymentType TEXT,
    amount NUMERIC(12, 2),
    declarationDate DATE,
    recordDate DATE,
    paymentDate DATE,
    currency TEXT,
    inserted TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ticker) REFERENCES tickers (ticker),
    CONSTRAINT unique_ticker_dividend UNIQUE (ticker, exOrEffDate, paymentType, amount, declarationDate, recordDate, paymentDate, currency)
);

CREATE INDEX IF NOT EXISTS idx_dividends_ticker ON dividends (ticker);

CREATE TABLE IF NOT EXISTS institutional_holdings (
    id SERIAL PRIMARY KEY,
    ticker text NOT NULL,
    sharesOutstandingPCT NUMERIC(12,2),
    sharesOutstandingTotal BIGINT,
    totalHoldingsValue BIGINT,
    increasedPositionsHolders BIGINT,
    increasedPositionsShares BIGINT,
    decreasedPositionsHolders BIGINT,
    decreasedPositionsShares BIGINT,
    heldPositionsHolders BIGINT,
    heldPositionsShares BIGINT,
    totalPositionsHolders BIGINT,
    totalPositionsShares BIGINT,
    newPositionsHolders  BIGINT,
    newPositionsShares BIGINT,
    soldOutPositionsHolders BIGINT,
    soldOutPositionsShares BIGINT,
    inserted TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ticker) REFERENCES tickers (ticker)
);

CREATE INDEX IF NOT EXISTS idx_institutional_holdings_ticker ON institutional_holdings (ticker);