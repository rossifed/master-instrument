INSERT INTO ref_data.entity_type (mnemonic, name) 
VALUES 
    ('COMP', 'Company')
ON CONFLICT (mnemonic) DO NOTHING;