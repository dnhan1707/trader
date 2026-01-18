-- Table for insider ownership data from Form 345 filings
CREATE TABLE insider_ownership (
    id SERIAL PRIMARY KEY,
    issuer_cik VARCHAR(10) NOT NULL,
    rpt_owner_cik VARCHAR(10) NOT NULL,
    direct_indirect_ownership CHAR(1) NOT NULL CHECK (direct_indirect_ownership IN ('D', 'I')),
    issuer_name TEXT NOT NULL,
    rpt_owner_name TEXT NOT NULL,
    shares DECIMAL(15, 2) DEFAULT 0,
    filing_date DATE NOT NULL,
    trans_code CHAR(1) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for efficient querying
-- Primary filter by company (issuer_cik)
CREATE INDEX idx_insider_issuer_cik ON insider_ownership (issuer_cik);

-- Filter by filing date for recent activity
CREATE INDEX idx_insider_filing_date ON insider_ownership (filing_date);

-- Group by reporting owner
CREATE INDEX idx_insider_rpt_owner ON insider_ownership (rpt_owner_cik);

-- Composite index for the main query pattern (company + date filtering)
CREATE INDEX idx_insider_company_date ON insider_ownership (issuer_cik, filing_date);

-- Composite index for aggregation queries
CREATE INDEX idx_insider_company_owner ON insider_ownership (issuer_cik, rpt_owner_cik, rpt_owner_name);

-- Index for sorting by shares
CREATE INDEX idx_insider_shares ON insider_ownership (shares DESC);

-- Comment explaining the table
COMMENT ON TABLE insider_ownership IS 'Insider ownership data from Form 345 filings, tracks executive and insider stock transactions';

COMMENT ON COLUMN insider_ownership.issuer_cik IS 'CIK of the company issuing the stock';
COMMENT ON COLUMN insider_ownership.rpt_owner_cik IS 'CIK of the reporting owner (insider)';
COMMENT ON COLUMN insider_ownership.direct_indirect_ownership IS 'D for direct ownership, I for indirect ownership';
COMMENT ON COLUMN insider_ownership.issuer_name IS 'Name of the company';
COMMENT ON COLUMN insider_ownership.rpt_owner_name IS 'Name of the reporting owner/insider';
COMMENT ON COLUMN insider_ownership.shares IS 'Number of shares held (can be fractional)';
COMMENT ON COLUMN insider_ownership.filing_date IS 'Date when the form was filed';
COMMENT ON COLUMN insider_ownership.trans_code IS 'Transaction code: P=Purchase, S=Sale, A=Grant/Award, D=Disposition to Issuer, F=Tax Withholding, M=Exercise of Derivative, G=Gift, J=Other, C=Conversion, W=Inheritance';