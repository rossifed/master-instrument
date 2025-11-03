INSERT INTO ref_data.venue_type (mnemonic, name) 
VALUES 
    ('EXCH', 'Exchange'),
    ('OTC', 'Over The Counter'),
    ('AGGR', 'Aggregated')

ON CONFLICT (mnemonic) DO NOTHING;