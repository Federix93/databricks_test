# Databricks notebook source
# MAGIC %md
# MAGIC #DESCRIPTION
# MAGIC
# MAGIC We have an OnPrem my SQL server database and we want to have the data fully freshed in databricks. So we enable a cdc (Change Data Capture)
# MAGIC
# MAGIC We have also a transaction system which is already sending streaming data to Databricks.
# MAGIC
# MAGIC The goal is to build a pipeline to ingest the data in databricks, clean, aggregate and show a dashboard on this data

# COMMAND ----------

# MAGIC %md
# MAGIC ###SETUP THE ENVIRONMENT AND GENERATE DATA

# COMMAND ----------

# MAGIC %run ./setup

# COMMAND ----------

# MAGIC %md
# MAGIC ###1 - Create a pipeline and attach the notebook pipeline to it in continous mode
# MAGIC ###2 - Run the pipeline
# MAGIC ###3 - Add new data with the cell below to see how the numbers change

# COMMAND ----------

# MAGIC %md
# MAGIC Add customers data

# COMMAND ----------

customers_columns = ['name', 'lastname', 'age', 'phone', 'email', 'city', 'country', 'countrynumber', 'currency', 'operation', 'operation_date', 'id']
data = [
    ("Alice", "Smith", '30', "1234567890", "alice@example.com", "New York", "USA", "+1", "USD", "APPEND", "2024-10-01 18:43:49", "C001"),
    ("Bob", "Johnson", '45', "9876543210", "bob@example.com", "Toronto", "Canada", "+1", "CAD", None, "2024-10-02 14:53:10", "C002"),
    ("Charlie", "Brown", '28', "5551234567", "charlie@example.com", "London", "UK", "+44", "GBP", "APPEND", "2024-10-03 11:25:22", "C003")
]

customers_volume = "/Volumes/mena/sales/customers_data_volume"
df = spark.createDataFrame(data, customers_columns)
df.write.format("json").mode("append").save(customers_volume)

# COMMAND ----------

# MAGIC %md
# MAGIC Add transactions data

# COMMAND ----------

transactions_columns = ['transaction_id', 'customer_id', 'transaction_date', 'amount', 'product_id', 'transaction_type', 'quantity']

data = [
    ("T001", "C001", "2024-05-20", 199.99, 10101, "New York", "Purchase", 2),
    ("T002", "C002", "2024-06-15", 349.50, 10102, "Toronto", "Refund", 1),
    ("T003", "C003", "2024-07-10", 89.00, 10103, "London", None, 3),
    ("T004", "C001", "2024-08-01", 120.75, 10104, "Los Angeles", "Unknown", 1),
    ("T005", "C004", "2024-08-18", 499.99, 10105, "Paris", "Purchase", 5)
]
transactions_volume = "/Volumes/mena/sales/transactions_data_volume"
df = spark.createDataFrame(data, transactions_columns)
df.write.format("json").mode("append").save(transactions_volume)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Schema enforcement

# COMMAND ----------

customers_columns = ['name', 'lastname', 'age', 'phone', 'email', 'city', 'country', 'countrynumber', 'currency', 'operation', 'operation_date', 'id']
data = [
    ("Alice", "Smith", '30', "1234567890", "alice@example.com", "New York", "USA", "+1", "USD", "APPEND", "2024-10-01 18:43:49", "C001", 'value1'),
    ("Bob", "Johnson", '45', "9876543210", "bob@example.com", "Toronto", "Canada", "+1", "CAD", None, "2024-10-02 14:53:10", "C002", 'value2'),
    ("Charlie", "Brown", '28', "5551234567", "charlie@example.com", "London", "UK", "+44", "GBP", "APPEND", "2024-10-03 11:25:22", "C003", 'value3')
]

customers_volume = "/Volumes/mena/sales/customers_data_volume"
df = spark.createDataFrame(data, customers_columns)
df.write.format("json").mode("append").save(customers_volume)


# COMMAND ----------

transactions_columns = ['transaction_id', 'customer_id', 'transaction_date', 'amount', 'product_id', 'transaction_type', 'quantity', 'new_column']

data = [
    ("T001", "C001", "2024-05-20", 199.99, 10101, "New York", "Purchase", 2, 'value1'),
    ("T002", "C002", "2024-06-15", 349.50, 10102, "Toronto", "Refund", 1, 'value2'),
    ("T003", "C003", "2024-07-10", 89.00, 10103, "London", "Purchase", 3, 'value3'),
    ("T004", "C001", "2024-08-01", 120.75, 10104, "Los Angeles", "Purchase", 1, 'value4'),
    ("T005", "C004", "2024-08-18", 499.99, 10105, "Paris", "Purchase", 5, 'value5')
]
transactions_volume = "/Volumes/mena/sales/transactions_data_volume"
df = spark.createDataFrame(data, transactions_columns)
df.write.format("json").mode("append").save(transactions_volume)