INSERT INTO instrument_type (mnemonic, name)
VALUES 
    ('EQ', 'Equity'),
    ('CRY', 'Crypto')
ON CONFLICT (mnemonic) DO NOTHING;