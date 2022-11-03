// Databricks notebook source
//create dataframe
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

val df = spark.read
.option("header","true")
.schema(customSchema)
.csv("/mnt/files/Employee.csv")


// COMMAND ----------

import org.apache.spark.sql.functions._
df.select(max("Department_id")).show()

// COMMAND ----------

df.select(min("Department_id")).show()

// COMMAND ----------

df.select(count("Department_id")).show()
df.select(countDistinct("Department_id")).show()

// COMMAND ----------

df.select(sum("Department_id")).show()
df.select(sumDistinct("Department_id")).show()

// COMMAND ----------

df
  .where(col("Department_id").isNotNull)
  .groupBy("Department_id")
  .agg(countDistinct("Employee_id")).as("Count_of_emp")
  .orderBy(asc("Department_id"))
  .show()

// using spark sql to write:
// select Department_id, count(Employee_id) as Count_of_emp
// from table
// group by 1
// order by 1 

// COMMAND ----------


