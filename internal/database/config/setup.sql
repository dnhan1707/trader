CREATE TABLE filings (
    accession_number TEXT PRIMARY KEY,
    cik TEXT NOT NULL,
    manager_name TEXT,
    report_period DATE NOT NULL,
    filing_date DATE NOT NULL
);

CREATE TABLE holdings (
    id BIGSERIAL PRIMARY KEY,
    accession_number TEXT NOT NULL,
    cusip TEXT NOT NULL,
    name_of_issuer TEXT,                                                        
    value NUMERIC,
    shares NUMERIC,
    share_type TEXT,
    CONSTRAINT unique_holding UNIQUE (accession_number, cusip, share_type)
    -- REMOVED: REFERENCES filings(accession_number) - will add this back later as a soft constraint
);

-- Create Indexes (CRITICAL for performance)
CREATE INDEX idx_holdings_cusip_shares ON holdings (cusip, shares DESC);
CREATE INDEX idx_holdings_accession ON holdings (accession_number); -- Added for joins
CREATE INDEX idx_filings_report_period ON filings (report_period);
CREATE INDEX idx_filings_manager ON filings (manager_name);
CREATE INDEX idx_filings_cik ON filings (cik); -- Added for lookups