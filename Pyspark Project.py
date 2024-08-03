# Databricks notebook source
# DBTITLE 1,Importar Dependências
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import regexp_replace, col

# COMMAND ----------

# DBTITLE 1,Criar Dataframe
df = spark.read.load('/FileStore/tables/googleplaystore.csv', format='csv', sep=',', header='true', escape='"', inferschema='true')

# COMMAND ----------

df.count()


# COMMAND ----------

df.show()

# COMMAND ----------

# DBTITLE 1,Checar Schema
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Data Cleaning
df = df.drop("Size", "Content Rating", "Last Updated", "Android Ver", "Current Ver")

# COMMAND ----------

df.show(5)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Convertendo Colunas para o seu tipo específico
df = df.withColumn("Reviews", col("Reviews").cast(IntegerType()))\
    .withColumn("Installs", regexp_replace(col("Installs"), "[^0-9]", ""))\
    .withColumn("Installs", col("Installs").cast(IntegerType()))\
        .withColumn("Price", regexp_replace(col("Price"), "[$]", ""))\
            .withColumn("Price", col("Price").cast('double'))

# COMMAND ----------

df.show(10)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.createOrReplaceTempView("Apps")

# COMMAND ----------

# MAGIC %sql select * from Apps

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos fazer uma análise para determinarmos alguns pontos.
# MAGIC 1. Top Reviews
# MAGIC 2. Top Installs App
# MAGIC 3. Category wise distribution of installed apps
# MAGIC 4. Top paid apps

# COMMAND ----------

# DBTITLE 1,Top Reviews
# MAGIC %sql select App,Type,sum(Reviews) from Apps 
# MAGIC group by App, Type
# MAGIC order by SUM(Reviews) desc
# MAGIC limit 10

# COMMAND ----------

# DBTITLE 1,Top Installs App
# MAGIC %sql select App,Type,sum(Installs) from Apps
# MAGIC group by App,Type
# MAGIC order by sum(Installs) desc
# MAGIC limit 10

# COMMAND ----------

# DBTITLE 1,Categories on installed
# MAGIC %sql select Category,SUM(Installs) from Apps
# MAGIC group by Category
# MAGIC order by SUM(Installs) desc

# COMMAND ----------

# DBTITLE 1,Top paid Apps
# MAGIC %sql select App,Price,SUM(Installs) from Apps
# MAGIC where Type='Paid'
# MAGIC group by App, Price
# MAGIC order by SUM(Installs) desc
# MAGIC limit 10
