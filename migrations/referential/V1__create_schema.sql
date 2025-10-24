CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS referential;

CREATE TABLE IF NOT EXISTS referential.instrument_type (
    instrument_type_id SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    mnemonic TEXT NOT NULL,
    name TEXT NOT NULL,
    CONSTRAINT uq_instrument_type_mnemonic UNIQUE (mnemonic)
);

CREATE TABLE IF NOT EXISTS referential.instrument (
    instrument_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    instrument_type_id SMALLINT NOT NULL,
    CONSTRAINT uq_instrument_symbol UNIQUE (symbol),
    CONSTRAINT fk_instrument_type FOREIGN KEY (instrument_type_id)
        REFERENCES referential.instrument_type (instrument_type_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS referential.equity (
    equity_id INT PRIMARY KEY,
    isin VARCHAR(12) NOT NULL,
    cusip VARCHAR(9) NOT NULL,
    CONSTRAINT uq_equity_isin UNIQUE (isin),
    CONSTRAINT fk_equity_instrument FOREIGN KEY (equity_id)
        REFERENCES referential.instrument (instrument_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
);
