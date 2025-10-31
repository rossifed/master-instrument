INSERT INTO referential.instrument_id_mapping (
    source,
    external_instrument_id
)
SELECT DISTINCT
    source,
    external_instrument_id
FROM staging.stg_qa_instrument_id
ON CONFLICT (source, external_instrument_id) DO NOTHING;
