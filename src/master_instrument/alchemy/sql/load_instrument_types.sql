INSERT INTO ref_data.instrument_type (mnemonic, name) 
VALUES 
    ('EQU', 'Equity'),
    ('BND', 'Bond'),
    ('CRY', 'Cryptocurrency'),
    ('FUT', 'Future'),
    ('FND', 'Fund')
ON CONFLICT (mnemonic) DO NOTHING;