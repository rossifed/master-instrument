BEGIN;

WITH stg_qa_quote_with_id AS (
  SELECT
    qm.internal_quote_id,
    sqq.external_quote_id,
    im.internal_instrument_id,
    vm.internal_venue_id,
    sqq.price_unit,
    sqq.mic,
    sqq.mic_desc AS market_name,
    sqq.delist_date,
    ccy.currency_id,
    sqq.is_primary,
    sqq.source
  FROM staging.stg_qa_quote sqq 
  LEFT JOIN ref_data.quote_mapping qm 
    ON qm.external_quote_id = sqq.external_quote_id 
    AND qm.source = sqq.source
  LEFT JOIN ref_data.instrument_mapping im
    ON im.external_instrument_id = sqq.external_instrument_id 
    AND im.source = sqq.source
  LEFT JOIN ref_data.venue_mapping vm
    ON vm.external_venue_id = sqq.external_venue_id
    AND vm.source = sqq.source
  JOIN ref_data.currency ccy 
    ON ccy.code = sqq.iso_curr_code
),
merge_quote AS (
  MERGE INTO ref_data.quote AS tgt
  USING stg_qa_quote_with_id src
  ON src.internal_quote_id = tgt.quote_id
  WHEN MATCHED THEN
    UPDATE SET
      instrument_id = src.internal_instrument_id,
      venue_id      = src.internal_venue_id,
      mic           = src.mic,
      market_name   = src.market_name,
      delisted_date = src.delist_date,
      currency_id   = src.currency_id,
      is_primary    = src.is_primary,
      price_unit    = src.price_unit
  WHEN NOT MATCHED THEN
    INSERT (
      instrument_id,
      venue_id,
      mic,
      market_name,
      delisted_date,
      currency_id,
      is_primary,
      price_unit
    )
    VALUES (
      src.internal_instrument_id,
      src.internal_venue_id,
      src.mic,
      src.market_name,
      src.delist_date,
      src.currency_id,
      src.is_primary,
      src.price_unit
    )
  RETURNING tgt.quote_id AS internal_quote_id, src.external_quote_id, src.source
),
update_mapping AS (
  INSERT INTO ref_data.quote_mapping (source, external_quote_id, internal_quote_id)
  SELECT
    source,
    external_quote_id,
    internal_quote_id
  FROM merge_quote
  ON CONFLICT (source, external_quote_id) DO NOTHING
)
SELECT 1;

COMMIT;