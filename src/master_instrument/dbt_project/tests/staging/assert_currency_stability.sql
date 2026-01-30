-- Test: Currency should not change for a quote over time
-- A quote trading in different currencies across time is a data quality issue
-- This is the critical test we discussed earlier
-- Returns quotes where currency changes

SELECT
    p."InfoCode" || '-' || p."ExchIntCode" AS external_quote_id,
    p."InfoCode",
    p."ExchIntCode",
    COUNT(DISTINCT p."ISOCurrCode") AS currency_count,
    STRING_AGG(DISTINCT p."ISOCurrCode", ', ' ORDER BY p."ISOCurrCode") AS currencies,
    'currency_changed' AS violation
FROM {{ source('raw', 'qa_DS2PrimQtPrc') }} p
WHERE p."ISOCurrCode" IS NOT NULL
GROUP BY p."InfoCode", p."ExchIntCode"
HAVING COUNT(DISTINCT p."ISOCurrCode") > 1
