# Databricks notebook source
# MAGIC %python
# MAGIC  # File location and type
# MAGIC  file_location = "/mnt/files/Employee.csv"
# MAGIC  file_type = "csv"
# MAGIC  
# MAGIC  # CSV options
# MAGIC  infer_schema = "false"
# MAGIC  first_row_is_header = "true"
# MAGIC  delimiter = ","
# MAGIC  
# MAGIC  # The applied options are for CSV files. For other file types, these will be ignored.
# MAGIC  df = spark.read.format(file_type) \
# MAGIC    .option("inferSchema", infer_schema) \
# MAGIC    .option("header", first_row_is_header) \
# MAGIC    .option("sep", delimiter) \
# MAGIC    .load(file_location)
# MAGIC  
# MAGIC  display(df)
# MAGIC  
# MAGIC  temp_table_name = "employee_csv"
# MAGIC  df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from employee_csv

# COMMAND ----------

# MAGIC %python
# MAGIC # File location and type
# MAGIC file_location = "/mnt/files/upsert_data.csv"
# MAGIC file_type = "csv"
# MAGIC 
# MAGIC # CSV options
# MAGIC infer_schema = "false"
# MAGIC first_row_is_header = "true"
# MAGIC delimiter = ","
# MAGIC 
# MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
# MAGIC df = spark.read.format(file_type) \
# MAGIC   .option("inferSchema", infer_schema) \
# MAGIC   .option("header", first_row_is_header) \
# MAGIC   .option("sep", delimiter) \
# MAGIC   .load(file_location)
# MAGIC 
# MAGIC display(df)
# MAGIC 
# MAGIC # Create a view or table
# MAGIC 
# MAGIC temp_table_name = "upsert_data_csv"
# MAGIC 
# MAGIC df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from upsert_data_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS DATAVOWELDB;
# MAGIC USE DATAVOWELDB

# COMMAND ----------

# MAGIC %sql
# MAGIC --LOCATION is where we want to store out data table
# MAGIC --from EMPLOYEE_CSV insert all data which the department_id is not null to this new data table
# MAGIC DROP TABLE IF EXISTS EMPLOYEE_DELTA_TABLE;
# MAGIC 
# MAGIC CREATE TABLE EMPLOYEE_DELTA_TABLE
# MAGIC USING delta
# MAGIC PARTITIONED BY (DEPARTMENT_ID)
# MAGIC LOCATION "/DATAVOWEL/DELTA/EMPLOYEE_DATA"
# MAGIC AS 
# MAGIC   (
# MAGIC   SELECT * FROM EMPLOYEE_CSV WHERE DEPARTMENT_ID IS NOT NULL
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL EMPLOYEE_DELTA_TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from EMPLOYEE_DELTA_TABLE; --988
# MAGIC select count(*) from UPSERT_DATA_CSV;  --17

# COMMAND ----------

# MAGIC %sql
# MAGIC -- IN EMPLOYEE_DELTA_TABLE THERE ARE 988 ROWS
# MAGIC --AFTER DOING MERGE OPERATION 998 ROWS WILL BE THERE AND 7 ROWS WILL BE UPDATED
# MAGIC MERGE INTO EMPLOYEE_DELTA_TABLE                            -- the MERGE instruction is used to perform the upsert
# MAGIC USING upsert_data_csv
# MAGIC 
# MAGIC ON EMPLOYEE_DELTA_TABLE.employee_id = upsert_data_csv.employee_id -- ON is used to describe the MERGE condition
# MAGIC    
# MAGIC WHEN MATCHED THEN                                           -- WHEN MATCHED describes the update behavior
# MAGIC   UPDATE SET
# MAGIC   EMPLOYEE_DELTA_TABLE.Last_Name = upsert_data_csv.Last_Name   
# MAGIC WHEN NOT MATCHED THEN                                       -- WHEN NOT MATCHED describes the insert behavior
# MAGIC   INSERT (Employee_id,First_Name,Last_Name,Gender,Salary,Date_of_Birth,Age,Country,Department_id,Date_of_Joining,Manager_id,Currency,End_Date)              
# MAGIC   VALUES (Employee_id,First_Name,Last_Name,Gender,Salary,Date_of_Birth,Age,Country,Department_id,Date_of_Joining,Manager_id,Currency,End_Date)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM EMPLOYEE_DELTA_TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY EMPLOYEE_DELTA_TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC --check the previous version data when employee_id = 12
# MAGIC SELECT * FROM EMPLOYEE_DELTA_TABLE VERSION AS OF 0 WHERE EMPLOYEE_ID = 12

# COMMAND ----------

# MAGIC %sql
# MAGIC --check the updated version data when employee_id = 12
# MAGIC SELECT * FROM EMPLOYEE_DELTA_TABLE VERSION AS OF 1 WHERE EMPLOYEE_ID = 12

# COMMAND ----------

# MAGIC %fs ls /DATAVOWEL/DELTA/EMPLOYEE_DATA/_delta_log

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_delta_table where employee_id in (21,35,45,47,49)

# COMMAND ----------

# MAGIC %sql
# MAGIC --update the table by setting first_name to null for above employee_id
# MAGIC UPDATE datavoweldb.EMPLOYEE_DELTA_TABLE SET First_Name = Null where employee_id in (21,35,45,47,49) 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_delta_table where employee_id in (21,35,45,47,49)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false

# COMMAND ----------

# MAGIC %sql
# MAGIC --vacuum is to prevent that user can go back to previous version. Once we set up with this, user won't go back to check any previous versions but the lastes version of data 
# MAGIC VACUUM DATAVOWELDB.employee_delta_table RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_delta_table version as of 2 where employee_id in (21,35,45,47,49)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_delta_table version as of 1 where employee_id in (21,35,45,47,49)

# COMMAND ----------


