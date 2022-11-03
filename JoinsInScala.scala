// Databricks notebook source
val dfEmp = spark.read
.option("header","true")
.csv("/mnt/files/Sample_data4jOIN.csv")

//Creating the dataframe by name dfDep
val dfDep = spark.read
.option("header","true")
.csv("/mnt/files/Department.csv")

// COMMAND ----------

display(dfEmp)

// COMMAND ----------

display(dfDep)

// COMMAND ----------

import org.apache.spark.sql.functions._
dfEmp.join(dfDep,dfEmp("Department_id") === dfDep("Department_id"),"inner")
.select(col("Employee_id"),dfEmp.col("Department_id"),dfDep.col("Department_id"),col("Name")).show()


// COMMAND ----------

dfEmp.join(dfDep,dfEmp("Department_id") === dfDep("Department_id"),"left")
.select(col("Employee_id"),dfEmp.col("Department_id"),dfDep.col("Department_id"),col("Name")).show()

// COMMAND ----------

dfEmp.join(dfDep,dfEmp("Department_id") === dfDep("Department_id"),"full")
.select(col("Employee_id"),dfEmp.col("Department_id"),dfDep.col("Department_id"),col("Name")).show()

// COMMAND ----------

dfEmp.createOrReplaceTempView("EMP")
dfDep.createOrReplaceTempView("DEPT")

// COMMAND ----------

val query1 = spark.sql("select * from Emp e INNER JOIN Dept d on e.department_id = d.department_id")
query1.show()

// COMMAND ----------

val query2 = spark.sql("select * from Emp e LEFT JOIN Dept d on e.department_id = d.department_id")
query2.show()

// COMMAND ----------

val query3 = spark.sql("select * from Emp e FULL JOIN Dept d on e.department_id = d.department_id")
query3.show()

// COMMAND ----------


