# Databricks notebook source
# MAGIC %md
# MAGIC INSTALL LIBRARIES
# MAGIC

# COMMAND ----------

# MAGIC %pip install Faker
# MAGIC %pip install pandas

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE SCHEMA AND VOLUMES

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS mena
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS mena.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS mena.sales.customers_data_volume

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS mena.sales.transactions_data_volume

# COMMAND ----------

# MAGIC %md
# MAGIC GENERATE SOME FAKE DATA

# COMMAND ----------

country_config_map = {
    "United States": {
        "locale": "en_US",
        "currency": "USD",
        "countrynumber": "+1",
        "cities": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
    },
    "United Kingdom": {
        "locale": "en_GB",
        "currency": "GBP",
        "countrynumber": "+44",
        "cities": ["London", "Manchester", "Birmingham", "Liverpool", "Glasgow"]
    },
    "Germany": {
        "locale": "de_DE",
        "currency": "EUR",
        "countrynumber": "+49",
        "cities": ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne"]
    },
    "France": {
        "locale": "fr_FR",
        "currency": "EUR",
        "countrynumber": "+33",
        "cities": ["Paris", "Marseille", "Lyon", "Toulouse", "Nice"]
    },
    "Spain": {
        "locale": "es_ES",
        "currency": "EUR",
        "countrynumber": "+34",
        "cities": ["Madrid", "Barcelona", "Valencia", "Seville", "Malaga"]
    },
    "Italy": {
        "locale": "it_IT",
        "currency": "EUR",
        "countrynumber": "+39",
        "cities": ["Rome", "Milan", "Naples", "Turin", "Florence"]
    },
    "Netherlands": {
        "locale": "nl_NL",
        "currency": "EUR",
        "countrynumber": "+31",
        "cities": ["Amsterdam", "Rotterdam", "The Hague", "Utrecht", "Eindhoven"]
    },
    "Belgium (French)": {
        "locale": "fr_BE",
        "currency": "EUR",
        "countrynumber": "+32",
        "cities": ["Brussels", "Liège", "Charleroi", "Antwerp", "Ghent"]
    },
    "Belgium (Dutch)": {
        "locale": "nl_BE",
        "currency": "EUR",
        "countrynumber": "+32",
        "cities": ["Brussels", "Antwerp", "Ghent", "Bruges", "Leuven"]
    },
    "Canada (English)": {
        "locale": "en_CA",
        "currency": "CAD",
        "countrynumber": "+1",
        "cities": ["Toronto", "Vancouver", "Montreal", "Ottawa", "Calgary"]
    },
    "Canada (French)": {
        "locale": "fr_CA",
        "currency": "CAD",
        "countrynumber": "+1",
        "cities": ["Montreal", "Quebec City", "Ottawa", "Toronto", "Vancouver"]
    },
    "Mexico": {
        "locale": "es_MX",
        "currency": "MXN",
        "countrynumber": "+52",
        "cities": ["Mexico City", "Guadalajara", "Monterrey", "Cancún", "Puebla"]
    },
    "Brazil": {
        "locale": "pt_BR",
        "currency": "BRL",
        "countrynumber": "+55",
        "cities": ["São Paulo", "Rio de Janeiro", "Brasília", "Salvador", "Fortaleza"]
    },
    "Argentina": {
        "locale": "es_AR",
        "currency": "ARS",
        "countrynumber": "+54",
        "cities": ["Buenos Aires", "Córdoba", "Rosario", "Mendoza", "La Plata"]
    },
    "Russia": {
        "locale": "ru_RU",
        "currency": "RUB",
        "countrynumber": "+7",
        "cities": ["Moscow", "Saint Petersburg", "Novosibirsk", "Yekaterinburg", "Nizhny Novgorod"]
    },
    "Poland": {
        "locale": "pl_PL",
        "currency": "PLN",
        "countrynumber": "+48",
        "cities": ["Warsaw", "Kraków", "Wrocław", "Gdańsk", "Poznań"]
    },
    "Czech Republic": {
        "locale": "cs_CZ",
        "currency": "CZK",
        "countrynumber": "+420",
        "cities": ["Prague", "Brno", "Ostrava", "Plzeň", "Liberec"]
    },
    "Turkey": {
        "locale": "tr_TR",
        "currency": "TRY",
        "countrynumber": "+90",
        "cities": ["Istanbul", "Ankara", "Izmir", "Bursa", "Antalya"]
    },
    "China": {
        "locale": "zh_CN",
        "currency": "CNY",
        "countrynumber": "+86",
        "cities": ["Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Chengdu"]
    },
    "Japan": {
        "locale": "ja_JP",
        "currency": "JPY",
        "countrynumber": "+81",
        "cities": ["Tokyo", "Osaka", "Kyoto", "Nagoya", "Sapporo"]
    },
    "South Korea": {
        "locale": "ko_KR",
        "currency": "KRW",
        "countrynumber": "+82",
        "cities": ["Seoul", "Busan", "Incheon", "Daegu", "Daejeon"]
    },
    "India": {
        "locale": "en_IN",
        "currency": "INR",
        "countrynumber": "+91",
        "cities": ["New Delhi", "Mumbai", "Bangalore", "Kolkata", "Chennai"]
    },
    "Australia": {
        "locale": "en_AU",
        "currency": "AUD",
        "countrynumber": "+61",
        "cities": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"]
    },
    "New Zealand": {
        "locale": "en_NZ",
        "currency": "NZD",
        "countrynumber": "+64",
        "cities": ["Auckland", "Wellington", "Christchurch", "Hamilton", "Dunedin"]
    },
    "South Africa": {
        "locale": "en_ZA",
        "currency": "ZAR",
        "countrynumber": "+27",
        "cities": ["Cape Town", "Johannesburg", "Durban", "Pretoria", "Port Elizabeth"]
    }
}


# COMMAND ----------

def generate_customers_data():
    fake = Faker()
    country = random.choice(list(country_config_map.keys()))
    config = country_config_map[country]
    
    name = fake.first_name()
    lastname = fake.last_name()
    age = str(random.randint(18, 65))
    phone = fake.phone_number()
    email = fake.email()
    city = fake.random_elements(elements=config["cities"], length=1)[0]
    countrynumber = config["countrynumber"]
    currency = config["currency"]
    operations = OrderedDict([("APPEND", 0.5),("UPDATE", 0.3),(None, 0.01)])
    operation = fake.random_elements(elements=operations, length=1)[0]
    operation_date = fake.date_time_this_month()
    id = str(uuid.uuid4())

    return (name, lastname, age, phone, email, city, country, countrynumber, currency, operation, operation_date, id)


# COMMAND ----------

def generate_transactions_data(df_customers):
    fake = Faker()
    transaction_id = fake.uuid4()
    customer_id = fake.random_elements(elements=df_customers['id'].tolist(), length=1)[0]
    fake_datetime = fake.date_time_this_month()
    transaction_date = fake_datetime
    amount = round(random.uniform(10, 500), 2)
    product_id = random.randint(1000000, 10000000)
    transaction_types = OrderedDict([("Purchase", 0.69),("Refund", 0.3),("Transfer", 0.01)])
    transaction_type = fake.random_elements(elements=transaction_types, length=1)[0]
    quantity = random.randint(1, 10) if transaction_type == "Purchase" else 0
    return (transaction_id, customer_id, transaction_date, amount, product_id, transaction_type, quantity)

# COMMAND ----------

from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType, StructField, StructType, TimestampType
from collections import OrderedDict 
import random
import uuid
import pandas as pd

num_customers = 100
num_transactions = 500
generate_customers_data_udf = udf(generate_customers_data, 
                       returnType=StringType())
generate_transactions_data_udf = udf(generate_transactions_data, 
                       returnType=StringType())

customers_columns = ['name', 'lastname', 'age', 'phone', 'email', 'city', 'country', 'countrynumber', 'currency', 'operation', 'operation_date', 'id']
transactions_columns = ['transaction_id', 'customer_id', 'transaction_date', 'amount', 'product_id', 'transaction_type', 'quantity']

customers_data = [generate_customers_data() for _ in range(num_customers)]

customer_df = spark.createDataFrame(customers_data, customers_columns)
transactions_data = [generate_transactions_data(pd.DataFrame(customers_data, columns=customers_columns)) for _ in range(num_transactions)]
transaction_df = spark.createDataFrame(transactions_data, transactions_columns)

customers_volume = "/Volumes/mena/sales/customers_data_volume"
transactions_volume = "/Volumes/mena/sales/transactions_data_volume"

customer_df.repartition(100).write.format("json").mode("append").save(customers_volume)
transaction_df.repartition(100).write.format("json").mode("append").save(transactions_volume)

