// Databricks notebook source
// create Schema
import org.apache.spark.sql.types._
val customSchema = StructType(
  List(
              StructField("Employee_id", StringType, true),
              StructField("First_Name", StringType, true),
              StructField("Last_Name", StringType, true),  
              StructField("Gender", StringType, true),
              StructField("Salary", StringType, true),
              StructField("Date_of_Birth", StringType, true),
              StructField("Age", StringType, true),
              StructField("Country", StringType, true),
              StructField("Department_id", StringType, true),
              StructField("Date_of_Joining", StringType, true),
              StructField("Manager_id", StringType, true),
              StructField("Currency", StringType, true),
              StructField("End_Date", StringType, true)
	)
  
)

// COMMAND ----------

// create dataframe
val df = spark.read
.option("header","true")
.schema(customSchema)
.csv("/mnt/files/Employee.csv")

// COMMAND ----------

import org.apache.spark.sql.functions._
// display all columns info:
df.select(col("*")).show()

// COMMAND ----------

//select specific columns:
df.select("Department_id","First_Name","Last_Name","Gender").show()

// COMMAND ----------

// add new column to current df
df
  .withColumn("Added Column",df.col("Gender"))
  .withColumnRenamed("Added Column","New Gender")
  .show()

// COMMAND ----------

//Drop an enxsited column
df
  .drop("New Gender")
  .show()

// COMMAND ----------

//create a new dfConv based on previous df but change some of the string columns to integer for operation 
val dfConv = df
                .withColumn("Department_id",df("Department_id").cast(IntegerType))
                .withColumn("Employee_id",df("Employee_id").cast(IntegerType))
                .withColumn("Age",df("Age").cast(IntegerType))
                .withColumn("Salary",df("Salary").cast(IntegerType))
                .withColumn("Manager_id",df("Manager_id").cast(IntegerType))

// COMMAND ----------

//pring out df
display(dfConv)

// COMMAND ----------

// print out df schema
dfConv.printSchema()

// COMMAND ----------


