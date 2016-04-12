// Databricks notebook source exported at Tue, 12 Apr 2016 14:31:07 UTC
// attach the library sql-jdbc-4.jar to the cluster

Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver") // check that the driver is available

// sql params
val jdbcUsername = "dataEng"
val jdbcPassword = "hosers" // tTw9XcX2Vh"
val jdbcHostname = "199.71.174.235" // <---- DW-1
val jdbcPort = 1433
val jdbcDatabase = "Census"   
val jdbcTable = "bd.Value"    

val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase};user=${jdbcUsername};password=${jdbcPassword};"

// Test the connection 
import scala.sys.process._
s"nc -z -w 5 $jdbcHostname $jdbcPort".!

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### Load data into Spark from DW-1

// COMMAND ----------

val dataValuesDF = sqlContext.read
    .format("jdbc")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("url", jdbcUrl)
    //.option("name", "censusValues")
    .option("dbtable", "bd.Value")
    .load()

val subCategoryDF = sqlContext.read
    .format("jdbc")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("url", jdbcUrl)
    .option("dbtable", "bd.SubCategory")
    .load()

val categoryDF = sqlContext.read
    .format("jdbc")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("url", jdbcUrl)
    .option("dbtable", "bd.Category")
    .load()

val valueTypeDF = sqlContext.read
    .format("jdbc")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("url", jdbcUrl)
    .option("dbtable", "bd.ValueType")
    .load()

val timePeriodDF = sqlContext.read
    .format("jdbc")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("url", jdbcUrl)
    .option("dbtable", "bd.TimePeriod")
    .load()

val personAttributesDF = sqlContext.read
    .format("jdbc")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("url", jdbcUrl)
    .option("dbtable", "bd.PersonAttributes")
    .load()

val ageRangeDF = sqlContext.read
    .format("jdbc")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("url", jdbcUrl)
    .option("dbtable", "bd.AgeRange")
    .load()

val maritalStatusDF = sqlContext.read
    .format("jdbc")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("url", jdbcUrl)
    .option("dbtable", "bd.MaritalStatus")
    .load()

val medicalStaffTypeDF = sqlContext.read
    .format("jdbc")
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("url", jdbcUrl)
    .option("dbtable", "bd.MedicalStaffType")
    .load()



println("Table: Values  : ROWS --> " + dataValuesDF.count)
println("Table: Sub-Cat  : ROWS --> " + subCategoryDF.count)
println("Table: Category  : ROWS --> " + categoryDF.count)
println("Table: Value-Type  : ROWS --> " + valueTypeDF.count)
println("Table: Time-Period  : ROWS --> " + timePeriodDF.count)
println("Table: Person-Attributes  : ROWS --> " + personAttributesDF.count)
println("Table: Age-Range  : ROWS --> " + ageRangeDF.count)
println("Table: Marital-Status  : ROWS --> " + maritalStatusDF.count)
println("Table: Medical-Staff-Type  : ROWS --> " + medicalStaffTypeDF.count)

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Profile the Data

// COMMAND ----------

display(dataValuesDF.describe())

// COMMAND ----------

dataValuesDF.select("subCategoryID").filter("subCategoryID is null").count

// COMMAND ----------

dataValuesDF.select("geonameid").filter("geonameid is null").count

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### Exlore the data

// COMMAND ----------

display(dataValuesDF)

// COMMAND ----------

display(subCategoryDF)

// COMMAND ----------

display(categoryDF)

// COMMAND ----------

display(valueTypeDF)

// COMMAND ----------

display(timePeriodDF)

// COMMAND ----------

display(personAttributesDF)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### Join the Data 

// COMMAND ----------

dataValuesDF.printSchema

// COMMAND ----------

display(dataValuesDF.filter("categoryId = 18"))

// COMMAND ----------

import org.apache.spark.sql.functions.{concat, lit}

val demographyDF = dataValuesDF
  .join(categoryDF, dataValuesDF("categoryId") === categoryDF("categoryId"), "leftouter")
  .drop(categoryDF.col("categoryId"))
  .withColumnRenamed("name", "category")
  .withColumnRenamed("description", "category_description")
  .join(subCategoryDF, dataValuesDF("subCategoryId") === subCategoryDF("subCategoryId"), "leftouter")
  .drop(subCategoryDF.col("subCategoryId"))
  .drop(subCategoryDF.col("categoryId"))
  .withColumnRenamed("description", "subCategory_description")
  .join(valueTypeDF, categoryDF("valueTypeId") === valueTypeDF("valueTypeId"), "leftouter")
  .drop(valueTypeDF.col("valueTypeId"))
  .join(timePeriodDF, dataValuesDF("timePeriodId") === timePeriodDF("timePeriodId"), "leftouter")
  .drop(timePeriodDF.col("timePeriodId"))
  .withColumn("timePeriod", concat(timePeriodDF("startDate"), lit(" - "), timePeriodDF("endDate")))
  .join(personAttributesDF, dataValuesDF("valueId") === personAttributesDF("valueId"), "leftouter")
  .drop(personAttributesDF.col("valueId"))
  .join(ageRangeDF, personAttributesDF("ageRangeId") === ageRangeDF("ageRangeId"), "leftouter")
  .drop(ageRangeDF.col("ageRangeId"))
  .join(maritalStatusDF, personAttributesDF("maritalStatusId") === maritalStatusDF("maritalStatusId"), "leftouter")
  .drop(maritalStatusDF.col("maritalStatusId"))
  .withColumnRenamed("status", "marital_status")
  .join(medicalStaffTypeDF, personAttributesDF("medicalStaffTypeID") === medicalStaffTypeDF("medicalStaffTypeID"), "leftouter")
  .drop(medicalStaffTypeDF.col("medicalStaffTypeId"))
  .withColumn("genderCol", personAttributesDF("isMale").cast("string"))   // cast Struct: terms --> String: terms


  //join --> geoName, place
  
demographyDF.printSchema
demographyDF.registerTempTable("demography")

// COMMAND ----------

display(demographyDF.describe())


// COMMAND ----------

display(demographyDF)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Count rows by data key 

// COMMAND ----------

// MAGIC %sql select count(1) as numRecords, category, categoryId, subCategoryId from demography group by category,categoryId, subCategoryId order by categoryId, subCategoryId

// COMMAND ----------

// MAGIC %sql select count(*) as numRecords, categoryId, subCategoryId, geonameId, placeId, timePeriodId, ageRangeId, isMale, maritalStatusId, medicalStaffTypeId from demography group by categoryId, subCategoryId, geonameId, placeId, timePeriodId, ageRangeId, isMale, maritalStatusId, medicalStaffTypeId order by numRecords desc 

// COMMAND ----------

val replacedDF = demographyDF.na.fill(0.0).na.fill("null")

val withKey = replacedDF.withColumn("key", concat(replacedDF("categoryId"), lit(" - "), replacedDF("subCategoryId"), lit(" - "), replacedDF("geoNameId"), lit(" - "), replacedDF("placeId"), lit(" - "), replacedDF("timePeriodId"), lit(" - "), replacedDF("ageRangeId"), lit(" - "), replacedDF("genderCol"), lit(" - "), replacedDF("maritalStatusId"), lit(" - "), replacedDF("medicalStaffTypeId")))


withKey.registerTempTable("keyVal")

// COMMAND ----------

// MAGIC %sql describe keyval

// COMMAND ----------

// MAGIC %sql select count(*) as numRecords, key from keyval group by key order by numRecords DESC

// COMMAND ----------

val numRecords = sqlContext.sql("select count(*) as numRecords, key from keyval group by key order by numRecords DESC")
numRecords.filter("numRecords < 1").show

// COMMAND ----------

// MAGIC %sql select category, value, key, valueType from keyval where categoryId = "$categoryId" order by value

// COMMAND ----------

// MAGIC %sql select category, subCategory, value, key, valueType from keyval where categoryId = "$categoryId" and subCategoryId = "$subCategoryId" order by value

// COMMAND ----------

// MAGIC %sql select timePeriodId, geonameId, count(*) as numRecords from census where categoryId = "$categoryId" and geonameId is not null group by timePeriodId, geonameId order by timePeriodId

// COMMAND ----------

// MAGIC %sql select * from census where categoryId = "$categoryId" and timePeriodId = "$TimePeriodId" and geonameId is not null order by geonameId

// COMMAND ----------

// MAGIC %sql select value, geonameId from census where categoryId = "$categoryId" and timePeriodId = "$TimePeriodId" and geonameId is not null order by value

// COMMAND ----------

val df = sqlContext.sql("select value from census where categoryId = 27 and timePeriodId = 12 and geonameId is not null")
display(df.describe())

// COMMAND ----------

dbutils.widgets.help

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### Example 2: Life Expectancy at Birth

// COMMAND ----------

display(dataValuesDF.filter("categoryid = 20").describe())

// COMMAND ----------

display(dataValuesDF.filter("categoryid = 20").select("value", "timePeriodId"))

// COMMAND ----------

display(dataValuesDF.filter("categoryid = 20").filter("timePeriodId = 6").select("value", "geonameId"))

// COMMAND ----------

dataValuesDF.filter("categoryid = 20").filter("timePeriodId = 6").filter("geonameid is null").show


// COMMAND ----------

display(dataValuesDF.filter("categoryid = 20").filter("timePeriodId = 6").filter("geonameid is not null").select("value", "geonameId"))


// COMMAND ----------

display(dataValuesDF.filter("categoryid = 20").filter("timePeriodId = 6").filter("geonameid is not null").select("value").describe())


// COMMAND ----------

// MAGIC %md 
// MAGIC #### Join all tables to a single dataframe, write to Spark SQL table

// COMMAND ----------

censusDF.select("valueType").distinct.show(truncate=false)

// COMMAND ----------

display(censusDF)

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Get basic statistics dashboard, by category

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Get advanced statistics dashboard, by category

// COMMAND ----------

df.show