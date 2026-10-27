DROP TABLE IF EXISTS fact_transactions;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_entity;

CREATE TABLE dim_entity (
 entity_key BIGINT PRIMARY KEY,
 entity_id UUID NOT NULL UNIQUE,
 first_name VARCHAR NOT NULL,
 last_name VARCHAR NOT NULL,
 contact_email VARCHAR NOT NULL,
 registration_date DATE NOT NULL,
 region VARCHAR NOT NULL
);
CREATE TABLE dim_date (
 date_key INTEGER PRIMARY KEY,
 full_date DATE NOT NULL UNIQUE,
 year INTEGER NOT NULL,
 quarter INTEGER NOT NULL,
 month INTEGER NOT NULL,
 month_name VARCHAR NOT NULL,
 day INTEGER NOT NULL
);
CREATE TABLE fact_transactions (
 transaction_id UUID PRIMARY KEY,
 entity_key BIGINT NOT NULL REFERENCES dim_entity(entity_key),
 date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
 category VARCHAR NOT NULL,
 transaction_amount DECIMAL(18,2) NOT NULL,
 currency VARCHAR NOT NULL,
 timestamp TIMESTAMPTZ NOT NULL
);

INSERT INTO dim_entity
SELECT row_number() OVER (ORDER BY entity_id)::BIGINT AS entity_key, entity_id,
       first_name, last_name, contact_email, registration_date, region
FROM silver_entities;

INSERT INTO dim_date
SELECT DISTINCT strftime(transaction_date, '%Y%m%d')::INTEGER AS date_key,
       transaction_date AS full_date, year(transaction_date) AS year,
       quarter(transaction_date) AS quarter, month(transaction_date) AS month,
       monthname(transaction_date) AS month_name, day(transaction_date) AS day
FROM silver_transactions t JOIN silver_entities e USING (entity_id);

INSERT INTO fact_transactions
SELECT t.transaction_id, e.entity_key, d.date_key, t.category, t.transaction_amount,
       t.currency, t.timestamp
FROM silver_transactions t JOIN dim_entity e USING (entity_id)
JOIN dim_date d ON d.full_date=t.transaction_date
QUALIFY row_number() OVER (PARTITION BY t.transaction_id ORDER BY t.timestamp, t.category)=1;
