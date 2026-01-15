-- schema: staging

DROP SCHEMA IF EXISTS "staging" CASCADE;

CREATE SCHEMA IF NOT EXISTS "staging";

CREATE TABLE staging.dim_account
(
	account_number BIGINT NOT NULL,
	account_name VARCHAR(255) NOT NULL,
	account_currency VARCHAR(5) NOT NULL,
    CONSTRAINT acct_nr_pk PRIMARY KEY (account_number)
);

CREATE TABLE "staging".axi_trade_history
(
	account_nr BIGINT NOT NULL,
	ticket BIGINT NOT NULL UNIQUE,
	open_datetime TIMESTAMP NOT NULL,
	close_datetime TIMESTAMP,
	open_date_key BIGINT NOT NULL,
	close_date_key BIGINT,
	type VARCHAR(25) NOT NULL,
	symbol VARCHAR(50),
	size NUMERIC,
	open_price NUMERIC,
	close_price NUMERIC,
	commission NUMERIC,
	swap NUMERIC,
	profit NUMERIC,
	stop_loss NUMERIC,
	take_profit NUMERIC,
	CONSTRAINT trade_hist_pk PRIMARY KEY (ticket),
	CONSTRAINT acct_nr_fk FOREIGN KEY (account_nr)
		REFERENCES staging.dim_account(account_number) MATCH FULL
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

-- schema: core
DROP SCHEMA IF EXISTS core CASCADE;

CREATE SCHEMA core;

-- schema: gold
DROP SCHEMA IF EXISTS access CASCADE;

CREATE SCHEMA IF NOT EXISTS access;

SELECT 'OPERATION COMPLETE!!'
