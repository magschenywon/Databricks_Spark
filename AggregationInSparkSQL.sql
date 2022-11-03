-- Databricks notebook source
-- MAGIC %scala
-- MAGIC //create schema and read df
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

select max(Department_id),min(Department_id),avg(Department_id),mean(Department_id) from viewEmployee;

-- COMMAND ----------

select count(Department_id),count(distinct Department_id) from viewEmployee;

-- COMMAND ----------

select sum(Department_id),sum(distinct Department_id) from viewEmployee;

-- COMMAND ----------

select Department_id, count(distinct Employee_id) as Number_of_Employees
from viewEmployee
where Department_id is not null
group by 1
order by 1 desc

-- COMMAND ----------


