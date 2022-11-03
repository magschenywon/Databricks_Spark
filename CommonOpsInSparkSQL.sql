-- Databricks notebook source
-- MAGIC %scala
-- MAGIC // create schema and read df 
-- MAGIC import org.apache.spark.sql.types._
-- MAGIC val customSchema = StructType(
-- MAGIC   List(
-- MAGIC               StructField("Employee_id", IntegerType, true),
-- MAGIC               StructField("First_Name", StringType, true),
-- MAGIC               StructField("Last_Name", StringType, true),  
-- MAGIC               StructField("Gender", StringType, true),
-- MAGIC               StructField("Salary", IntegerType, true),
-- MAGIC               StructField("Date_of_Birth", StringType, true),
-- MAGIC               StructField("Age", IntegerType, true),
-- MAGIC               StructField("Country", StringType, true),
-- MAGIC               StructField("Department_id", IntegerType, true),
-- MAGIC               StructField("Date_of_Joining", StringType, true),
-- MAGIC               StructField("Manager_id", IntegerType, true),
-- MAGIC               StructField("Currency", StringType, true),
-- MAGIC               StructField("End_Date", StringType, true)
-- MAGIC 	)
-- MAGIC   
-- MAGIC )
-- MAGIC 
-- MAGIC val df = spark.read
-- MAGIC .option("header","true")
-- MAGIC .schema(customSchema)
-- MAGIC .csv("/mnt/files/Employee.csv")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC //create a view
-- MAGIC df.createOrReplaceTempView("viewEmployee")

-- COMMAND ----------

select * from viewEmployee

-- COMMAND ----------

select * from viewEmployee where Department_id = 1

-- COMMAND ----------

select * from viewEmployee where Department_id between 1 and 10 

-- COMMAND ----------

select * from viewEmployee where First_Name = 'Ugo'

-- COMMAND ----------

select * from viewEmployee where Department_id is null;

-- COMMAND ----------

select * from viewEmployee where Department_id is not null;

-- COMMAND ----------

select Employee_id, First_Name, Last_Name, Gender, First_Name as New_Name from viewEmployee

-- COMMAND ----------

select cast(Employee_id as string), First_Name, Last_Name, Gender from viewEmployee

-- COMMAND ----------


