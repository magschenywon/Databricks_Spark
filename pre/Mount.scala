// Databricks notebook source
val containerName = "files"
val storageAccountName = "dataovowelstorageacct"
val sas = "?sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2022-12-09T00:23:58Z&st=2022-11-03T15:23:58Z&spr=https&sig=4ftg1G7xRpzWuMXGlE6ELaxdbA0Fsm789CwdgbBCNL0%3D"
val url = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/"
var config = "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net"

// COMMAND ----------

// COMMAND ----------

dbutils.fs.mount(
source = url,
mountPoint = "/mnt/files",
extraConfigs = Map(config -> sas))

// COMMAND ----------

// MAGIC %fs ls /mnt/files
