// Databricks notebook source
// MAGIC %scala
// MAGIC //create scehma and read df
// MAGIC import org.apache.spark.sql.types._
// MAGIC val customSchema = StructType(
// MAGIC   List(
// MAGIC               StructField("Employee_id", IntegerType, true),
// MAGIC               StructField("First_Name", StringType, true),
// MAGIC               StructField("Last_Name", StringType, true),  
// MAGIC               StructField("Gender", StringType, true),
// MAGIC               StructField("Salary", IntegerType, true),
// MAGIC               StructField("Date_of_Birth", StringType, true),
// MAGIC               StructField("Age", IntegerType, true),
// MAGIC               StructField("Country", StringType, true),
// MAGIC               StructField("Department_id", IntegerType, true),
// MAGIC               StructField("Date_of_Joining", StringType, true),
// MAGIC               StructField("Manager_id", IntegerType, true),
// MAGIC               StructField("Currency", StringType, true),
// MAGIC               StructField("End_Date", StringType, true)
// MAGIC 	)
// MAGIC   
// MAGIC )
// MAGIC 
// MAGIC val df = spark.read
// MAGIC .option("header","true")
// MAGIC .schema(customSchema)
// MAGIC .csv("/mnt/files/Employee.csv")

// COMMAND ----------

// MAGIC %scala
// MAGIC //create a global temp view
// MAGIC df.createOrReplaceGlobalTempView("viewGlobalEmployee")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from global_temp.viewGlobalEmployee

// COMMAND ----------

spark.sql("select * from global_temp.viewGlobalEmployee").show()

// COMMAND ----------


