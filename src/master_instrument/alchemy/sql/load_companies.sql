BEGIN;

WITH stg_qa_company_with_id AS (
  SELECT
    cim.internal_company_id,
    sqc.external_id,
    'QA' AS source,
    sqc."PrimaryName" AS name,
    1 AS entity_type_id
  FROM staging.stg_qa_company sqc
  LEFT JOIN ref_data.company_mapping cim 
    ON cim.external_company_id = sqc.external_id
   AND cim.source = 'QA'
),
merge_entity AS (
  MERGE INTO ref_data.entity AS tgt
  USING stg_qa_company_with_id src
  ON src.internal_company_id = tgt.entity_id
  WHEN MATCHED THEN
    UPDATE SET 
      name = src.name,
      entity_type_id = src.entity_type_id
  WHEN NOT MATCHED THEN
    INSERT (name, entity_type_id)
    VALUES (src.name, src.entity_type_id)
  RETURNING entity_id, src.external_id, src.source
),
enriched_entity AS (
  SELECT
    me.entity_id,
    src.name,
    src.entity_type_id,
    me.external_id,
    me.source
  FROM merge_entity me
  JOIN stg_qa_company_with_id src
    ON src.external_id = me.external_id
),
merge_company AS (
  MERGE INTO ref_data.company AS tgt
  USING enriched_entity src
  ON tgt.company_id = src.entity_id
  WHEN MATCHED THEN
    UPDATE SET 
      name = src.name
  WHEN NOT MATCHED THEN
    INSERT (company_id, name)
    VALUES (src.entity_id, src.name)
),
update_mapping AS (
  INSERT INTO ref_data.company_mapping (
    source, external_company_id, internal_company_id
  )
  SELECT
    source, external_id, entity_id
  FROM merge_entity
  ON CONFLICT DO NOTHING
)
SELECT 1;

COMMIT;

