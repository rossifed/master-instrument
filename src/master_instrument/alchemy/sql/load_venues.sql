BEGIN;

WITH stg_qa_venue_with_id AS (
  SELECT
    vm.internal_venue_id,
    sqe.external_exchange_id AS external_venue_id,
    sqe.source,
    sqe.exch_mnem AS mnemonic,
    sqe.exch_name AS name,
    ctry.country_id,
    typ.venue_type_id
  FROM staging.stg_qa_exchange sqe
  JOIN ref_data.venue_type typ 
    ON typ.mnemonic = 'EXCH'
  LEFT JOIN ref_data.venue_mapping vm 
    ON vm.external_venue_id = sqe.external_exchange_id
   AND vm.source = sqe.source
  LEFT JOIN ref_data.country ctry 
    ON ctry.code = sqe.exch_ctry_code
),
merge_venue AS (
  MERGE INTO ref_data.venue AS tgt
  USING stg_qa_venue_with_id AS src
  ON src.internal_venue_id = tgt.venue_id
  WHEN MATCHED THEN
    UPDATE SET 
      name = src.name,
      mnemonic = src.mnemonic,
      venue_type_id = src.venue_type_id,
      country_id = src.country_id
  WHEN NOT MATCHED THEN
    INSERT (mnemonic, name, venue_type_id, country_id)
    VALUES (src.mnemonic, src.name, src.venue_type_id, src.country_id)
  RETURNING venue_id, src.external_venue_id, src.source
),
enriched_venue AS (
  SELECT
    mv.venue_id,
    src.name,
    src.mnemonic,
    src.venue_type_id,
    src.country_id,
    mv.external_venue_id,
    mv.source
  FROM merge_venue mv
  JOIN stg_qa_venue_with_id src
    ON src.external_venue_id = mv.external_venue_id
),
update_mapping AS (
  INSERT INTO ref_data.venue_mapping (source, external_venue_id, internal_venue_id)
  SELECT source, external_venue_id, venue_id
  FROM merge_venue
  ON CONFLICT DO NOTHING
)

SELECT 1;

COMMIT;