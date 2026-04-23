#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "=== PostgreSQL: table counts ==="
docker compose exec -T postgres psql -U bd_user -d bd_lab2 <<'SQL'
\pset pager off
SELECT 'stg_mock_data_raw' AS table_name, count(*) AS rows FROM bd_lab2.stg_mock_data_raw
UNION ALL SELECT 'dim_customer', count(*) FROM bd_lab2.dim_customer
UNION ALL SELECT 'dim_seller', count(*) FROM bd_lab2.dim_seller
UNION ALL SELECT 'dim_product', count(*) FROM bd_lab2.dim_product
UNION ALL SELECT 'dim_store', count(*) FROM bd_lab2.dim_store
UNION ALL SELECT 'dim_supplier', count(*) FROM bd_lab2.dim_supplier
UNION ALL SELECT 'dim_date', count(*) FROM bd_lab2.dim_date
UNION ALL SELECT 'fact_sales', count(*) FROM bd_lab2.fact_sales
ORDER BY table_name;
SQL

echo
echo "=== PostgreSQL: fact FK null checks ==="
docker compose exec -T postgres psql -U bd_user -d bd_lab2 <<'SQL'
\pset pager off
SELECT
  sum((customer_key IS NULL)::int) AS customer_key_nulls,
  sum((seller_key IS NULL)::int) AS seller_key_nulls,
  sum((product_key IS NULL)::int) AS product_key_nulls,
  sum((store_key IS NULL)::int) AS store_key_nulls,
  sum((supplier_key IS NULL)::int) AS supplier_key_nulls,
  sum((sale_date_key IS NULL)::int) AS sale_date_key_nulls
FROM bd_lab2.fact_sales;
SQL

echo
echo "=== PostgreSQL: store/supplier key checks ==="
docker compose exec -T postgres psql -U bd_user -d bd_lab2 <<'SQL'
\pset pager off
SELECT
  (SELECT count(DISTINCT concat_ws('||', store_name, store_location, store_city, store_country, store_phone, store_email))
   FROM bd_lab2.stg_mock_data_raw) AS source_store_key_dist,
  (SELECT count(*) FROM bd_lab2.dim_store) AS dim_store_rows,
  (SELECT count(*)
   FROM (
     SELECT store_name, store_location, store_city, store_country, store_phone, store_email
     FROM bd_lab2.stg_mock_data_raw
     GROUP BY store_name, store_location, store_city, store_country, store_phone, store_email
     HAVING count(DISTINCT coalesce(store_state, '<NULL>')) > 1
   ) t) AS source_store_key_multi_attrs;

SELECT
  (SELECT count(DISTINCT concat_ws('||', supplier_name, supplier_email, supplier_phone, supplier_country))
   FROM bd_lab2.stg_mock_data_raw) AS source_supplier_key_dist,
  (SELECT count(*) FROM bd_lab2.dim_supplier) AS dim_supplier_rows,
  (SELECT count(*)
   FROM (
     SELECT supplier_name, supplier_email, supplier_phone, supplier_country
     FROM bd_lab2.stg_mock_data_raw
     GROUP BY supplier_name, supplier_email, supplier_phone, supplier_country
     HAVING count(DISTINCT concat_ws('||', supplier_contact, supplier_address, supplier_city)) > 1
   ) t) AS source_supplier_key_multi_attrs;
SQL

echo
echo "=== ClickHouse: report counts ==="
docker compose exec -T clickhouse clickhouse-client \
  --user bd_ch_user \
  --password bd_ch_pass \
  --database bd_lab2 \
  -q "
SELECT *
FROM (
    SELECT 'report_sales_by_products' AS table_name, count() AS rows FROM report_sales_by_products
    UNION ALL
    SELECT 'report_sales_by_customers', count() FROM report_sales_by_customers
    UNION ALL
    SELECT 'report_sales_by_time', count() FROM report_sales_by_time
    UNION ALL
    SELECT 'report_sales_by_stores', count() FROM report_sales_by_stores
    UNION ALL
    SELECT 'report_sales_by_suppliers', count() FROM report_sales_by_suppliers
    UNION ALL
    SELECT 'report_product_quality', count() FROM report_product_quality
)
ORDER BY table_name;"
