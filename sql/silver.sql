CREATE OR REPLACE TABLE silver_entities AS
WITH ranked AS (
 SELECT entity_id, first_name, last_name, contact_email, registration_date,
        upper(trim(region)) AS region,
        row_number() OVER (PARTITION BY entity_id ORDER BY source_hash, loaded_at) AS rn
 FROM bronze_entities
)
SELECT entity_id,
       upper(left(trim(first_name), 1)) || lower(substr(trim(first_name), 2)) AS first_name,
       upper(left(trim(last_name), 1)) || lower(substr(trim(last_name), 2)) AS last_name,
       lower(trim(contact_email)) AS contact_email, registration_date, region
FROM ranked WHERE rn = 1;

CREATE OR REPLACE TABLE silver_transactions AS
SELECT transaction_id, entity_id, trim(category) AS category, transaction_amount,
       currency, timestamp, CAST(timestamp AT TIME ZONE 'UTC' AS DATE) AS transaction_date
FROM bronze_transactions
QUALIFY row_number() OVER (PARTITION BY transaction_id ORDER BY source_hash, loaded_at) = 1;

CREATE OR REPLACE TABLE rejected_orphan_transactions AS
SELECT t.transaction_id, t.entity_id, 'entity_id_not_found_in_silver_entities' AS reason
FROM silver_transactions t LEFT JOIN silver_entities e USING (entity_id)
WHERE e.entity_id IS NULL;
