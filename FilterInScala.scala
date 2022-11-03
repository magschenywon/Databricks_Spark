// Databricks notebook source
// create schema
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

// create dataframe withou schema
val df = spark.read
.option("header","true")
.schema(customSchema)
.csv("/mnt/files/Employee.csv")

// COMMAND ----------

display(df)

// COMMAND ----------

// create a fileter to select department which id = 1
df.filter($"Department_id" === 1).show()
// same way to filter without $ sign:
// df.filter("Department_id == 1").show()

// COMMAND ----------

// filter out data which dept_id != 1:
df.filter("Department_id != 1").show()
// df.filter($"Department_id" !== 1).show()

// COMMAND ----------

// filter out and select data which dept_id > 1 and < 10
df.filter($"Department_id" > 1 and $"Department_id" < 10).show()
// df.where($"Department_id" > 1 and $"Department_id" < 10).show()

// df.filter($"Department_id" >1)
// .filter($"Department_id" < 10)
// .show()

// df.where($"Department_id" >1)
// .where($"Department_id" < 10)
// .show()

// COMMAND ----------

df.filter($"First_Name" === "Ugo").show()

// COMMAND ----------

// filter using library package
import org.apache.spark.sql.functions._
df.filter(df("Department_id") === 1).show()
// df.filter(col("Department_id") === 1).show()

// COMMAND ----------

df.filter(df("Department_id").isNull).show()
// df.filter(col("Department_id").isNull).show()

// COMMAND ----------


