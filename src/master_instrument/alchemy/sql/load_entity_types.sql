MERGE INTO ref_data.entity_type AS target
USING (
    VALUES 
        ('CMPY', 'Company')
) AS incoming(mnemonic, name)
ON target.mnemonic = incoming.mnemonic
WHEN NOT MATCHED THEN
  INSERT (mnemonic, name)
  VALUES (incoming.mnemonic, incoming.name);
