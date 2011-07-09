CREATE TABLE ledger (
    xtn_date date,
    checknum character varying,
    note text,
    account text,
    commodity text,
    amount numeric,
    cleared character varying(1),
    tags text
);

CREATE INDEX idx_ledger_account ON ledger USING btree (account);