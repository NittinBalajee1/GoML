-- query: monthly_revenue_by_category
-- Amounts are grouped by currency: values in different currencies are not additive.
SELECT category, currency, sum(transaction_amount) AS total_transaction_amount
FROM fact_transactions
WHERE CAST(timestamp AT TIME ZONE 'UTC' AS DATE) >= date_trunc('month', ?::DATE)::DATE
  AND CAST(timestamp AT TIME ZONE 'UTC' AS DATE) < (date_trunc('month', ?::DATE) + INTERVAL '1 month')::DATE
GROUP BY category, currency ORDER BY category, currency;

-- query: top_regions_average_per_entity
-- First compute each entity's mean, then equally weight entity means by region/currency.
WITH entity_means AS (
 SELECT e.region, f.currency, e.entity_key, avg(f.transaction_amount) AS entity_average
 FROM fact_transactions f JOIN dim_entity e USING (entity_key)
 GROUP BY e.region, f.currency, e.entity_key
), region_means AS (
 SELECT region, currency, avg(entity_average) AS average_transaction_amount_per_entity
 FROM entity_means GROUP BY region, currency
), ranked_regions AS (
 SELECT region, currency, average_transaction_amount_per_entity,
        row_number() OVER (PARTITION BY currency ORDER BY average_transaction_amount_per_entity DESC, region) AS region_rank
 FROM region_means
)
SELECT region, currency, average_transaction_amount_per_entity
FROM ranked_regions WHERE region_rank <= 5
ORDER BY currency, average_transaction_amount_per_entity DESC, region;
