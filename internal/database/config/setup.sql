CREATE TABLE institutional_ownership (
    cik VARCHAR(10) NOT NULL,
    cusip VARCHAR(9) NOT NULL,
    name_of_issuer TEXT,
    shares_held BIGINT,
    total_value_usd NUMERIC(20, 2),
    manager_name TEXT,
    PRIMARY KEY (cik, cusip)
);

CREATE INDEX idx_ownership_cusip ON institutional_ownership (cusip);