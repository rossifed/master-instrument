{{ config(severity='warn') }}

-- Test: Each instrument must have exactly one primary exchange quote in staging
-- No primary quote or multiple primary quotes causes issues
-- Returns instruments with 0 or more than 1 primary quote

WITH instrument_primary_counts AS (
    SELECT
        e.external_instrument_id,
        e.ticker AS instrument_name,
        COUNT(q.external_quote_id) FILTER (WHERE q.is_primary_exchange = true) AS primary_quote_count,
        STRING_AGG(q.external_quote_id, ', ' ORDER BY q.external_quote_id) FILTER (WHERE q.is_primary_exchange = true) AS primary_quote_ids,
        STRING_AGG(q.mic, ', ' ORDER BY q.external_quote_id) FILTER (WHERE q.is_primary_exchange = true) AS venues
    FROM {{ ref('stg_qa_equity') }} e
    LEFT JOIN {{ ref('stg_qa_quote') }} q
        ON q.external_instrument_id = e.external_instrument_id
    GROUP BY e.external_instrument_id, e.ticker
)

SELECT
    external_instrument_id,
    instrument_name,
    primary_quote_count,
    primary_quote_ids,
    venues,
    CASE 
        WHEN primary_quote_count = 0 THEN 'no_primary_quote'
        WHEN primary_quote_count > 1 THEN 'multiple_primary_quotes'
    END AS violation
FROM instrument_primary_counts
WHERE primary_quote_count != 1
