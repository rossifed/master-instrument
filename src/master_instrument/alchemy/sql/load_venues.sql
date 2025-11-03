BEGIN;

WITH stg_qa_venue_with_id AS (
  SELECT
    vm.internal_venue_id ,
    sqe.external_exchange_id as external_venue_id,
    'QA' AS source,
    sqe."ExchMnem" as mnemonic,
    sqe."ExchName" AS name,
    1 AS venue_type_id
  FROM staging.stg_qa_exchange sqe
  LEFT JOIN ref_data.venue_mapping vm 
    ON vm.external_venue_id = sqe.external_exchange_id
   AND vm.source = 'QA'
),
merge_venue AS (
  MERGE INTO ref_data.venue AS tgt
  USING stg_qa_venue_with_id as src
  ON src.internal_venue_id = tgt.venue_id
  WHEN MATCHED THEN
    UPDATE SET 
      name = src.name,
      mnemonic = src.mnemonic,
      venue_type_id = src.venue_type_id
  WHEN NOT MATCHED THEN
    INSERT (mnemonic,name, venue_type_id)
    VALUES (src.mnemonic, src.name, src.venue_type_id)
  RETURNING venue_id, src.external_venue_id, src.source
),
enriched_venue AS (
  SELECT
    mv.venue_id,
    src.name,
    src.mnemonic,
    src.venue_type_id,
    mv.external_venue_id,
    mv.source
  FROM merge_venue mv
  JOIN stg_qa_venue_with_id src
    ON src.external_venue_id = mv.external_venue_id
),
--merge_exchange AS (
 -- MERGE INTO ref_data.exchange AS tgt
 -- USING enriched_venue src
--  ON tgt.venue_id = src.venue_id
 -- WHEN MATCHED THEN
 --   UPDATE SET 
 --     name = src.name
 -- WHEN NOT MATCHED THEN
 --   INSERT (venue_id, name)
 --   VALUES (src.venue_id, src.name)
--),
update_mapping AS (
  INSERT INTO ref_data.venue_mapping (
    source, external_venue_id, internal_venue_id
  )
  SELECT
    source, external_venue_id, venue_id
  FROM merge_venue
  ON CONFLICT DO NOTHING
)
SELECT 1;

COMMIT;

