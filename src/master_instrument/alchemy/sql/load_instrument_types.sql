MERGE INTO ref_data.instrument_type AS target
USING (
    VALUES 
        ('EQU', 'Equity'),
        ('BND', 'Bond'),
        ('CRY', 'Cryptocurrency'),
        ('FUT', 'Future'),
        ('FND', 'Fund')
) AS incoming(mnemonic, name)
ON target.mnemonic = incoming.mnemonic
WHEN NOT MATCHED THEN
  INSERT (mnemonic, name)
  VALUES (incoming.mnemonic, incoming.name);
