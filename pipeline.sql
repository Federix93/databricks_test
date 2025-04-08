-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE customer_bronze (
    name string,
    lastname string, 
    age string,
    phone string, 
    email string,
    city string,
    country string, 
    countrynumber string,
    currency string,
    operation string,
    operation_date string,
    id string,
    _rescued_data string
  ) TBLPROPERTIES ("quality" = "bronze") COMMENT "New customer data incrementally ingested from cloud object storage landing zone" AS
SELECT
  *
FROM
  STREAM read_files(
    "/Volumes/mena/sales/customers_data_volume",
    format => "json",
    inferColumnTypes => "true",
    rescuedDataColumn => "_rescued_data",
    schemaEvolutionMode => "rescue"
  );

-- COMMAND ----------

CREATE OR REFRESH TEMPORARY STREAMING LIVE TABLE customer_bronze_clean_v(
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed bronze customer view (i.e. what will become Silver)"
AS SELECT * 
FROM STREAM(customer_bronze);

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE customer_silver
TBLPROPERTIES ("quality" = "silver")
COMMENT "Clean, merged customers";

-- COMMAND ----------

APPLY CHANGES INTO customer_silver
FROM stream(customer_bronze_clean_v)
  KEYS (id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY operation_date 
  COLUMNS * EXCEPT (operation, operation_date, _rescued_data)
  STORED AS SCD TYPE 2;

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE transactions_bronze (
    transaction_id string,
    customer_id string, 
    transaction_date string,
    amount double, 
    product_id long,
    transaction_type string,
    quantity long,
    _rescued_data string
  ) TBLPROPERTIES ("quality" = "bronze") COMMENT "Transactions data incrementally ingested from cloud object storage landing zone" AS
SELECT
  *
FROM
  STREAM read_files(
    "/Volumes/mena/sales/transactions_data_volume",
    format => "json",
    inferColumnTypes => "true",
    rescuedDataColumn => "_rescued_data",
    schemaEvolutionMode => "rescue"
  );

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE transactions_silver(
  CONSTRAINT valid_id EXPECT (transaction_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_operation EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_transaction_type EXPECT (transaction_type IS NOT NULL AND transaction_type IN ('Transfer', 'Refund', 'Purchase'))
)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed bronze transactions view (i.e. what will become Silver)"
AS SELECT * 
FROM STREAM(transactions_bronze);

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE sales_by_age_group
TBLPROPERTIES ("quality" = "gold")
COMMENT "Sales by age group trend"
AS SELECT 
    CASE 
        WHEN age BETWEEN '18' AND 24 THEN '18-24'
        WHEN age BETWEEN '25' AND 34 THEN '25-34'
        WHEN age BETWEEN '35' AND 44 THEN '35-44'
        WHEN age BETWEEN '45' AND 54 THEN '45-54'
        WHEN age BETWEEN '55' AND 64 THEN '55-64'
        WHEN age >= '65' THEN '65+'
    END AS age_group,
    SUM(t.amount) AS total_sales
FROM 
    transactions_silver t
JOIN 
    customer_silver c ON t.customer_id = c.id
GROUP BY 
    age_group;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE sales_by_day_of_week
TBLPROPERTIES ("quality" = "gold")
COMMENT "Sales by day of week"
AS SELECT 
    EXTRACT(DOW FROM TO_DATE(transaction_date)) AS day_of_week,
    SUM(amount) AS total_sales
FROM 
    transactions_silver
GROUP BY 
    day_of_week
ORDER BY 
    day_of_week;


-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE customer_lifetime_value
TBLPROPERTIES ("quality" = "gold")
COMMENT "Sales by day of week"
AS SELECT 
    c.id AS customer_id,
    SUM(t.amount) AS total_spent
FROM 
    transactions_silver t
JOIN 
    customer_silver c ON t.customer_id = c.id
GROUP BY 
    c.id;