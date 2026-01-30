{{ config(
    materialized = "table",
    meta = {
        "dagster": {
            "asset_key": ["staging","qa","fundamental","stg_qa_std_financial_item"],
            "group": "fundamental"
        }
    }
) }}

SELECT 
    item."Item" AS item,
    item."StmtTypeCode" AS stmt_type_code,
    item."Desc_" AS desc,
    item."ItemPrecision" AS item_precision,
    (item."IsCurrency"=1) AS is_currency,
    'QA' AS source
FROM {{ source('raw', 'qa_RKDFndStdItem') }} item
WHERE item."Desc_" IS NOT NULL
  AND item."DataType" IN ('Float', 'Numeric')
