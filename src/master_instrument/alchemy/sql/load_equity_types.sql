MERGE INTO ref_data.equity_type AS target
USING (    
  	SELECT mnemonic, description
    FROM staging.stg_qa_equity_type
) AS source
ON target.mnemonic = source.mnemonic
WHEN MATCHED THEN
        UPDATE SET 
        description = source.description,
        mnemonic = source.mnemonic
WHEN NOT MATCHED THEN
    INSERT (mnemonic, description)
    VALUES (source.mnemonic, source.description);

