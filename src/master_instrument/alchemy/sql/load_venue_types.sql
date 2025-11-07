MERGE INTO ref_data.venue_type AS target
USING (
    VALUES 
        ('EXCH', 'Exchange'),
        ('OTC', 'Over The Counter'),
        ('AGGR', 'Aggregated')
) AS incoming(mnemonic, name)
ON target.mnemonic = incoming.mnemonic
WHEN NOT MATCHED THEN
  INSERT (mnemonic, name)
  VALUES (incoming.mnemonic, incoming.name);
