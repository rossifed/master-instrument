BEGIN;

WITH stg_qa_instrument_with_id AS (
    SELECT
    iim.internal_instrument_id,
    sqc.external_instrument_id,
    sqc.source,
    cim.internal_company_id AS entity_id,
    sqc.name,
    sqc.isin,
    sqc.cusip,
    sqc.sedol,
    sqc.ric,
    sqc.ticker,
    sqc.equity_type,
    sqc.description,
    sqc.div_unit,
    sqc.is_major_sec,
    ctry.country_id,
    sqc.split_date,
    sqc.split_factor,
    it.instrument_type_id
  FROM equity_source sqc
  LEFT JOIN ref_data.instrument_mapping iim 
    ON iim.external_instrument_id = sqc.external_instrument_id
   AND iim.source = sqc.source
  LEFT JOIN ref_data.company_mapping cim
    ON cim.external_company_id = sqc.external_company_id
   AND cim.source = sqc.source
  LEFT JOIN ref_data.country ctry
    ON ctry.code = sqc."Region"
  LEFT JOIN ref_data.instrument_type it
    ON it.mnemonic = "EQU"
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
    INSERT (name, instrument_type_id, entity_id, description)
    VALUES (src.name, src.instrument_type_id, src.entity_id, src.description)
  RETURNING tgt.instrument_id, src.external_instrument_id, src.source
),
enriched_instrument AS (
  SELECT
    me.instrument_id,
    src.isin,
    src.cusip,
    src.sedol,
    src.ric,
    src.ticker,
    src.equity_type,
    src.description,
    src.div_unit,
    src.is_major_sec,
    src.country_id,
    src.split_date,
    src.split_factor,
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
      isin = src.isin,
      cusip = src.cusip,
      sedol = src.sedol,
      ric = src.ric,
      ticker = src.ticker,
      equity_type = src.equity_type,
      description = src.description,
      div_unit = src.div_unit,
      is_major_sec = src.is_major_sec,
      country_id = src.country_id,
      split_date = src.split_date,
      split_factor = src.split_factor
  WHEN NOT MATCHED THEN
    INSERT (
      equity_id, isin, cusip, sedol, ric, ticker, equity_type,
      description, div_unit, is_major_sec, country_id,
      split_date, split_factor
    )
    VALUES (
      src.instrument_id, src.isin, src.cusip, src.sedol, src.ric, src.ticker,
      src.equity_type, src.description, src.div_unit, src.is_major_sec,
      src.country_id, src.split_date, src.split_factor
    )
),
update_mapping AS (
  INSERT INTO ref_data.instrument_mapping (
    source, external_instrument_id, internal_instrument_id
  )
  SELECT
    source, external_instrument_id, instrument_id
  FROM merge_instrument_base
  ON CONFLICT (source, external_instrument_id) DO NOTHING
)

SELECT 1;

COMMIT;
