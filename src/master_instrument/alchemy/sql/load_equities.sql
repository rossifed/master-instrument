BEGIN;

WITH stg_qa_instrument_with_id AS (
  SELECT
    iim.internal_instrument_id,
    sqc.external_instrument_id,
    'QA' AS source,
    cim.internal_company_id as enity_id,
    sqc."DsSecName" AS name,
    sqc.isin,
    1 AS instrument_type_id,
    cim.internal_company_id AS entity_id
  FROM staging.stg_qa_equity_instrument sqc
  LEFT JOIN ref_data.instrument_mapping iim 
    ON iim.external_instrument_id = sqc.external_instrument_id
   AND iim.source = 'QA'
  LEFT JOIN ref_data.company_mapping cim
    ON cim.external_company_id = sqc.external_company_id
   AND cim.source = 'QA'
),
merge_instrument_base AS (
  MERGE INTO ref_data.instrument AS tgt
  USING stg_qa_instrument_with_id src
  ON src.internal_instrument_id = tgt.instrument_id
  WHEN MATCHED THEN
    UPDATE SET 
      name = src.name,
      instrument_type_id = src.instrument_type_id,
      entity_id = src.entity_id
  WHEN NOT MATCHED THEN
    INSERT (name, instrument_type_id, entity_id)
    VALUES (src.name, src.instrument_type_id, src.entity_id)
  RETURNING instrument_id, src.external_instrument_id, src.source
),
enriched_instrument AS (
  SELECT
    me.instrument_id,
    src.isin,
    src.instrument_type_id,
    me.external_instrument_id,
    me.source
  FROM merge_instrument_base me
  JOIN stg_qa_instrument_with_id src
    ON src.external_instrument_id = me.external_instrument_id
),
merge_equity AS (
  MERGE INTO ref_data.equity AS tgt
  USING enriched_instrument src
  ON src.instrument_id = tgt.equity_id
  WHEN MATCHED THEN
    UPDATE SET 
      isin = src.isin
  WHEN NOT MATCHED THEN
    INSERT (equity_id, isin)
    VALUES (src.instrument_id, src.isin)
),
update_mapping AS (
  INSERT INTO ref_data.instrument_mapping (
    source, external_instrument_id, internal_instrument_id
  )
  SELECT
    source, external_instrument_id, instrument_id
  FROM merge_instrument_base
  ON CONFLICT DO NOTHING
)
SELECT 1;

COMMIT;