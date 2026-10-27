CREATE TABLE IF NOT EXISTS bronze_entities (
 entity_id UUID, first_name VARCHAR, last_name VARCHAR, contact_email VARCHAR,
 registration_date DATE, region VARCHAR, source_hash VARCHAR, loaded_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS bronze_transactions (
 transaction_id UUID, entity_id UUID, category VARCHAR, transaction_amount DECIMAL(18,2),
 currency VARCHAR, timestamp TIMESTAMPTZ, source_hash VARCHAR, loaded_at TIMESTAMP
);
