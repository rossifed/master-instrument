BEGIN;

WITH stg_qa_quote_with_id AS (
  SELECT
    qm.internal_quote_id,
    sqq.external_instrument_id,
    im.internal_instrument_id,
    sqq.external_quote_id,
    vm.internal_venue_id,
    'QA' AS source,
    CASE WHEN sqq."IsPrimExchQt" = 'Y' THEN TRUE ELSE FALSE END AS is_primary,
    sqq."PriceUnit" AS price_unit,
    sqq.mic,
    sqq."DelistDate" AS delisted_date,
    sqq."ISOCurrCode" AS currency
  FROM staging.stg_qa_quote sqq
  LEFT JOIN ref_data.quote_mapping qm 
    ON qm.external_quote_id = sqq.external_quote_id 
    AND qm.source = 'QA'
  LEFT JOIN ref_data.instrument_mapping im
    ON im.external_instrument_id = sqq.external_instrument_id 
    AND im.source = 'QA'
  LEFT JOIN ref_data.venue_mapping vm
    ON vm.external_venue_id = sqq.external_venue_id
    AND vm.source = 'QA'
),
merge_quote AS (
  MERGE INTO ref_data.quote AS tgt
  USING stg_qa_quote_with_id src
  ON src.internal_quote_id = tgt.quote_id
  WHEN MATCHED THEN
    UPDATE SET 
      instrument_id = src.internal_instrument_id,
      venue_id = src.internal_venue_id,
      delisted_date = src.delisted_date,
      currency = src.currency,
      mic = src.mic,
      price_unit = src.price_unit,
      is_primary = src.is_primary
  WHEN NOT MATCHED THEN
    INSERT (instrument_id, venue_id, is_primary, currency, mic, price_unit, delisted_date)
    VALUES (src.internal_instrument_id, src.internal_venue_id, src.is_primary, src.currency, src.mic, src.price_unit, src.delisted_date)
  RETURNING quote_id AS internal_quote_id, src.external_quote_id, src.source
),
update_mapping AS (
  INSERT INTO ref_data.quote_mapping (
    source, external_quote_id, internal_quote_id
  )
  SELECT
    source, external_quote_id, internal_quote_id
  FROM merge_quote
  ON CONFLICT (source, external_quote_id) DO NOTHING
)
SELECT 1;

COMMIT;
