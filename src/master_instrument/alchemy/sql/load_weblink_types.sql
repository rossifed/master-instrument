MERGE INTO ref_data.weblink_type AS target
USING (
    SELECT  code, description
    FROM staging.stg_qa_weblink_type
) AS source
ON target.weblink_type_id = source.Code
WHEN MATCHED THEN
    UPDATE SET description = source.description
WHEN NOT MATCHED THEN
    INSERT (weblink_type_id,description)
    VALUES (source.Code, source.description);