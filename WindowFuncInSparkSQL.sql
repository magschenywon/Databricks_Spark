-- Databricks notebook source
-- MAGIC %scala
-- MAGIC //create schema and read df
-- MAGIC import org.apache.spark.sql.types._
-- MAGIC val customSchema1 = StructType(
-- MAGIC         List(
-- MAGIC               StructField("Country", StringType, true),
-- MAGIC               StructField("Gender", StringType, true),
-- MAGIC               StructField("Employee_id", IntegerType, true),
-- MAGIC               StructField("First_Name", StringType, true),
-- MAGIC               StructField("Salary", IntegerType, true),  
-- MAGIC               StructField("Department_id", IntegerType, true)
-- MAGIC           
-- MAGIC 	))
-- MAGIC 
-- MAGIC val df = spark.read
-- MAGIC .option("header","true")
-- MAGIC .schema(customSchema1)
-- MAGIC .csv("/mnt/files/SampleEmployee.csv")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC df.createOrReplaceTempView("viewSampleEmployee")

-- COMMAND ----------

select 
Country,
Gender,
Employee_id,
First_Name,
Salary,
Department_id,
Max(Salary) over(Partition by Country) as Max_Salary,
Min(Salary) over(Partition by Country) as Min_Salary
from viewSampleEmployee
group by 1,2,3,4,5,6



-- COMMAND ----------

select 
Country,
Gender,
Employee_id,
First_Name,
Salary,
Department_id,
Max(Salary) over(partition by Country) as Max_Salary,
Min(Salary) over(partition by Country) as Min_Salary,
Row_number() over(partition by Country order by Salary desc) as row_number
from viewSampleEmployee
group by 1,2,3,4,5,6

-- COMMAND ----------

select * from (
  select 
  Country,
  Gender,
  Employee_id,
  First_Name,
  Salary,
  Department_id,
  Max(Salary) over(partition by Country) as Max_Salary,
  Min(Salary) over(partition by Country) as Min_Salary,
  Row_number() over(partition by Country order by Salary desc) as row_number
  from viewSampleEmployee
) t 
where t.row_number = 3

-- COMMAND ----------

select 
Country,
Gender,
Employee_id,
First_Name,
Salary,
Department_id,
rank() over(partition by Country order by Gender) as rk,
dense_rank() over(partition by Country order by Gender) as dense_rk
from viewSampleEmployee
group by 1,2,3,4,5,6

-- COMMAND ----------


