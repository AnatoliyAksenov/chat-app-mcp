### DDL script for HIVE or for Apache Spark SQL

DDL Scrip for Hive or for Apache Spark SQL

```sql
CREATE DATABASE IF NOT EXISTS raw_orders;

DROP TABLE IF EXISTS raw_orders.customers;

CREATE EXTERNAL TABLE IF NOT EXISTS raw_orders.customers (
  customer_id STRING COMMENT 'Customer ID',
  company_name STRING COMMENT 'Company name',
  contact_name STRING COMMENT 'Contract name',
  email STRING COMMENT 'Email address',
  phone STRING COMMENT 'Phone number',
  address STRING COMMENT 'Customers address',
  city STRING COMMENT 'Customers city',
  country STRING COMMENT 'Customers country',
  created_by STRING COMMENT 'Creator username',
  created_at TIMESTAMP COMMENT 'Creation time'
)
COMMENT 'Customers table'
PARTITIONED BY (load_date STRING)
STORED AS ORC
LOCATION 's3a://warehouse/raw_test/customers';

MSCK REPAIR TABLE raw_orders.customers;
```
