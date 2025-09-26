CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS referential;

CREATE TABLE IF NOT EXISTS referential.instrument_type (
    id SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    mnemonic TEXT NOT NULL,
    name TEXT NOT NULL,
    CONSTRAINT uq_instrument_type_mnemonic UNIQUE (mnemonic)
);

CREATE TABLE IF NOT EXISTS referential.instrument (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    instrument_type_id SMALLINT NOT NULL,
    CONSTRAINT uq_instrument_symbol UNIQUE (symbol),
    CONSTRAINT fk_instrument_type FOREIGN KEY (instrument_type_id)
        REFERENCES referential.instrument_type (id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
);


