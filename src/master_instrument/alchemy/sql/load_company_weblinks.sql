MERGE INTO ref_data.company_weblink AS target
USING (
    SELECT
        cm.internal_company_id AS company_id,
        sqcw.url,
        sqcw.weblink_type_id,
        sqcw.last_updated
    FROM staging.stg_qa_comapny_weblink AS sqcw
    JOIN ref_data.company_mapping AS cm
        ON cm.external_company_id = sqcw.external_company_id
) AS source
ON target.company_id = source.company_id
   AND target.weblink_type_id = source.weblink_type_id
WHEN MATCHED THEN
    UPDATE SET
        url = source.url,
        last_updated =source.last_updated
WHEN NOT MATCHED THEN
    INSERT (company_id, weblink_type_id, url, last_updated)
    VALUES (source.company_id, source.weblink_type_id, source.url, source.last_updated);