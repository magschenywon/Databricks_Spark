// Databricks notebook source
import com.databricks.spark.xml._ 

val df = spark.read
    .option("rootTag","dataset").option("rowTag","record")
    .xml("/mnt/files/EMPXML.xml")

// COMMAND ----------

display(df)

// COMMAND ----------

//flat the First_Name and Last_Name
val dfFlat = df
                .select(
                  "Employee_id"
                  ,"Age"
                  ,"Customer.First_Name"
                  ,"Customer.Last_Name"
                  ,"Gender"
                  ,"Salary"
                ,"Date_Of_Birth"
                ,"Age"
                ,"Country"
                ,"Department"
                ,"Date_Of_Joining"
                ,"Manager_id"
                ,"Currency"
                ,"End_Date"
                )

// COMMAND ----------

display(dfFlat)

// COMMAND ----------

dfFlat.createOrReplaceTempView("EMP")

// COMMAND ----------

df.printSchema()

// COMMAND ----------

// use spark.sql to cast Employee_id from long to int, age from long to int, Salary from long to int, Manager_id from long to int
val dfCast = spark.sql("select cast(Employee_id as int),cast(Age as int),First_Name,Last_Name,Gender,cast(Salary as int),Date_Of_Birth,cast(Age as int),Country,Department,Date_Of_Joining,cast(Manager_id as int),Currency,End_Date from EMP")

// COMMAND ----------


