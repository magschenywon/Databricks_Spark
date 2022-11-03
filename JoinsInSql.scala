// Databricks notebook source
// MAGIC %scala
// MAGIC val dfEmp = spark.read
// MAGIC .option("header","true")
// MAGIC .csv("/mnt/files/Sample_data4jOIN.csv")
// MAGIC 
// MAGIC //Creating the dataframe by name dfDep
// MAGIC val dfDep = spark.read
// MAGIC .option("header","true")
// MAGIC .csv("/mnt/files/Department.csv")

// COMMAND ----------

dfEmp.createOrReplaceTempView("EMP")
dfDep.createOrReplaceTempView("DEPT")

// COMMAND ----------

// MAGIC %sql
// MAGIC select e.Department_id,
// MAGIC d.Department_id,
// MAGIC d.Name
// MAGIC from EMP as e
// MAGIC inner join
// MAGIC DEPT as d 
// MAGIC on e.Department_id = d.Department_id

// COMMAND ----------

// MAGIC %sql
// MAGIC select e.Department_id,
// MAGIC d.Department_id,
// MAGIC d.Name
// MAGIC from EMP as e
// MAGIC left join
// MAGIC DEPT as d 
// MAGIC on e.Department_id = d.Department_id

// COMMAND ----------

// MAGIC %sql
// MAGIC select e.Department_id,
// MAGIC d.Department_id,
// MAGIC d.Name
// MAGIC from EMP as e
// MAGIC full join
// MAGIC DEPT as d 
// MAGIC on e.Department_id = d.Department_id

// COMMAND ----------


