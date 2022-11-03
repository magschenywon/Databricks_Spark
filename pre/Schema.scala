// Databricks notebook source
// preview all the data files
%fs ls /mnt/files

// COMMAND ----------

// load dataframe
val df = spark.read
.option("header","true")
.csv("/mnt/files/Employee.csv")

// COMMAND ----------

// create schema, change type of the columns
import org.apache.spark.sql.types._
val customSchema = StructType(
  List(
          StructField("Employee_id", IntegerType, true),
          StructField("First_Name", StringType, true),
          StructField("Last_Name", StringType, true),
          StructField("Gender", StringType, true),
          StructField("Salary", IntegerType, true),
          StructField("Date_of_Birth", StringType, true),
          StructField("Age", IntegerType, true),
          StructField("Country", StringType, true),
          StructField("Department_id", IntegerType, true),
          StructField("Date_of_Joining", StringType, true),
          StructField("Manager_id", IntegerType, true),
          StructField("Currency", StringType, true),
          StructField("End_Date", StringType, true)
      )
)

// COMMAND ----------

// attach customSchema to dataframe
val dfSchema = spark.read
.option("header","true")
.schema(customSchema)
.csv("/mnt/files/Employee.csv")

// COMMAND ----------

display(dfSchema)

// COMMAND ----------

// MAGIC %python
// MAGIC # // using python to creare schema
// MAGIC from pyspark.sql.types import *
// MAGIC customSchemaPython = StructType(
// MAGIC      [
// MAGIC           StructField("Employee_id", IntegerType(), True),
// MAGIC           StructField("First_Name", StringType(), True),
// MAGIC           StructField("Last_Name", StringType(), True),
// MAGIC           StructField("Gender", StringType(), True),
// MAGIC           StructField("Salary", IntegerType(), True),
// MAGIC           StructField("Date_of_Birth", StringType(), True),
// MAGIC           StructField("Age", IntegerType(), True),
// MAGIC           StructField("Country", StringType(), True),
// MAGIC           StructField("Department_id", IntegerType(), True),
// MAGIC           StructField("Date_of_Joining", StringType(), True),
// MAGIC           StructField("Manager_id", IntegerType(), True),
// MAGIC           StructField("Currency", StringType(), True),
// MAGIC           StructField("End_Date", StringType(), True)
// MAGIC      ]
// MAGIC   )

// COMMAND ----------

// MAGIC %python
// MAGIC # attach schema to dataframe
// MAGIC df = spark.read.format("csv") \
// MAGIC .options(header = 'true', delimiter = ',') \
// MAGIC .schema(customSchemaPython) \
// MAGIC .load("/mnt/files/Employee.csv")

// COMMAND ----------

// df.show()
display(df)
