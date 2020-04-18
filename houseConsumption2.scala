// Databricks notebook source
val dfPrepHouse = sqlContext
  .read.format("com.databricks.spark.csv")
  .option("header", "true")
  .load("/FileStore/tables/18100073.csv")

// COMMAND ----------

val dfHouseOnly = dfPrepHouse.filter($"New housing price indexes" ==="House only")
val houseSelectColumns = dfHouseOnly.select($"REF_DATE", $"GEO", $"VALUE")
val houseSelectCountry = houseSelectColumns.filter($"GEO" === "Canada" || $"GEO" ==="Ontario" ||$"GEO" ==="Newfoundland and Labrador" || $"GEO" ==="Prince Edward Island" || $"GEO" ==="Nova Scotia" || $"GEO" ==="New Brunswick" || $"GEO" === "Quebec" ||$"GEO" === "Manitoba" || $"GEO" === "Saskatchewan" || $"GEO"==="Alberta" ||$"GEO" ==="British Columbia" ||$"GEO" ==="Yukon" ||$"GEO" ==="Northwest Territories" || $"GEO" ==="Nunavut")
display(houseSelectCountry)

// COMMAND ----------

import org.apache.spark.sql.functions.split 
val houseSplit =  houseSelectCountry.withColumn("_tmp", split($"REF_DATE", "\\-")).select(
  $"_tmp".getItem(0).as("Year"),
  $"_tmp".getItem(1).as("col2"),
 $"GEO",
  $"VALUE"
).drop("_tmp")
val drop = houseSplit.dropDuplicates("Year","col2","GEO")
val houseCast = drop.select(
  drop.col("Year").cast("integer"),
  drop.col("col2").cast("integer"),
  drop.col("GEO").cast("string"),
  drop.col("VALUE").cast("integer"))

val houseToRename = Seq("Year","col2","Location","Price")
val houseRenamed = houseCast.toDF(houseToRename:_*)

val houseSort = houseRenamed.sort($"Year".asc,$"col2".asc)

val houseYear= houseSort.filter($"Year" >1985 &&$"Year" <2011)

import org.apache.spark.sql.{functions => func}
val df2 = houseYear.groupBy($"Year", $"Location").agg(func.sum($"Price"))

val houseRename = df2.sort($"Year".asc)
val house = houseRename.withColumnRenamed("sum(Price)","Price")
display(house)

// COMMAND ----------

//house.count
house.columns.size

// COMMAND ----------

house.createOrReplaceTempView("house")

// COMMAND ----------

val dfcons = sqlContext
  .read.format("com.databricks.spark.csv")
  .option("header", "true")
  .load("/FileStore/tables/36100225.csv")

val conPrice = dfcons.filter($"Prices"==="Current prices")

val conEstimates = conPrice.filter($"Estimates" ==="Household final consumption expenditure" || $"Estimates" ==="Food" || $"Estimates" ==="Recreation and culture" ||$"Estimates" ==="Clothing and footwear" ||$"Estimates" ==="Education")

val conSelect = conEstimates.select($"REF_DATE", $"GEO", $"Estimates",$"VALUE")

val conSelectCountry = conSelect.filter($"GEO" === "Canada" || $"GEO" ==="Ontario" ||$"GEO" ==="Newfoundland and Labrador" || $"GEO" ==="Prince Edward Island" || $"GEO" ==="Nova Scotia" || $"GEO" ==="New Brunswick" || $"GEO" === "Quebec" ||$"GEO" === "Manitoba" || $"GEO" === "Saskatchewan" || $"GEO"==="Alberta" ||$"GEO" ==="British Columbia")

val conToRename = Seq("Year","Location","Estimates","Household_consumption")
val conRenamed = conSelectCountry.toDF(conToRename:_*)

val conYear= conRenamed.filter($"Year" >1985 &&$"Year" <2011)

val conCast = conYear.select(
  conYear.col("Year").cast("integer"),
  conYear.col("Location").cast("string"),
  conYear.col("Estimates").cast("string"),
  conYear.col("Household_consumption").cast("integer")
  )

val con =conCast.sort($"Year".asc,$"Location")
display(con)

// COMMAND ----------

val conPrice = dfcons.filter($"Prices"==="Current prices")
val conSelectAll = conPrice.select($"REF_DATE", $"GEO", $"Estimates",$"VALUE")
val conCastAll = conSelectAll.select(
  conSelectAll.col("REF_DATE").cast("integer"),
  conSelectAll.col("GEO").cast("string"),
  conSelectAll.col("Estimates").cast("string"),
  conSelectAll.col("Value").cast("integer")
  )
val conEstimatesAll = conCastAll.filter($"Estimates" !=="Household final consumption expenditure")
//display(conSelectAll)
display(conEstimatesAll)


// COMMAND ----------

val conEstimatesAllSort = conEstimatesAll.sort($"Value".desc)
display(conEstimatesAllSort)


// COMMAND ----------

con.count
con.columns.size

// COMMAND ----------

val join = house.join(con, Seq("Year","Location"))
display(join)

// COMMAND ----------

//household consumption other year in Canada

val joinCanada = join.filter($"Location" ==="Canada")
display(joinCanada)

// COMMAND ----------

//house price other year in Canada
display(joinCanada)

// COMMAND ----------

//household consumption (all) other Years
display(joinCanada)

// COMMAND ----------

display(joinCanada)

// COMMAND ----------

//household consumption (total) other house price in canada
val joinCanadaTotal = joinCanada.filter($"Estimates" === "Household final consumption expenditure")
display(joinCanadaTotal)

// COMMAND ----------

//household comsumption by estimates in Canada
val joinCanadaOther = joinCanada.filter($"Estimates" !== "Household final consumption expenditure")
display(joinCanadaOther)

// COMMAND ----------

//hoisehold consumption - Estimaates by year in canada
display(joinCanadaOther)

// COMMAND ----------

val conCanadaStartEnd = joinCanadaOther.filter($"Year" === 1986 || $"Year" === 2010 )
display(conCanadaStartEnd)

// COMMAND ----------

//price by Location
val joinProvinces = join.filter($"Location" !== "Canada")
display(joinProvinces)

// COMMAND ----------

display(joinProvinces)

// COMMAND ----------

//ousehold consumption - total in Provinces by Year
val joinProvincesTotal = joinProvinces.filter($"Estimates" === "Household final consumption expenditure")
display(joinProvincesTotal)

// COMMAND ----------

//Housing price by Location in Canada
//Most expensive – Alberta
//Least expensive – Prince Edvard Island

val sortPrice =joinProvincesTotal.sort($"Price".desc)
display(sortPrice)


// COMMAND ----------

display(joinProvincesTotal)

// COMMAND ----------

val joinProvincesOther = joinProvinces.filter($"Estimates" !== "Household final consumption expenditure")
display(joinProvincesOther)

// COMMAND ----------

display(joinCanadaTotal)

// COMMAND ----------

// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC %md
// MAGIC •Load sample data
// MAGIC 
// MAGIC •Prepare and visualize data for ML algorithms
// MAGIC 
// MAGIC •Run a linear regression model
// MAGIC 
// MAGIC •Evaluation a linear regression model
// MAGIC 
// MAGIC •Visualize a linear regression model

// COMMAND ----------

joinCanadaTotal.createOrReplaceTempView("joinCanadaTotal")
joinProvinces.createOrReplaceTempView("joinProvinces")
joinProvincesTotal.createOrReplaceTempView("joinProvincesTotal")
joinCanada.createOrReplaceTempView("joinCanada")

// COMMAND ----------

// MAGIC %python
// MAGIC df= spark.sql("select * from joinCanadaTotal")

// COMMAND ----------

// MAGIC %python
// MAGIC corrCanadaTotal= spark.sql("select Year, Price, Household_consumption from joinCanadaTotal")
// MAGIC corrProvincesTotal= spark.sql("select Year, Price, Household_consumption from joinProvincesTotal")

// COMMAND ----------

// MAGIC %md
// MAGIC check for correlation

// COMMAND ----------

// MAGIC 
// MAGIC %python
// MAGIC #corr for CanadaTotal
// MAGIC import six
// MAGIC for i in corrCanadaTotal.columns:
// MAGIC     if not( isinstance(corrCanadaTotal.select(i).take(1)[0][0], six.string_types)):
// MAGIC         print( "Correlation to price for ", i, corrCanadaTotal.stat.corr('Price',i))

// COMMAND ----------

// MAGIC 
// MAGIC %python
// MAGIC from pyspark.ml import Pipeline
// MAGIC from pyspark.ml.feature import VectorAssembler
// MAGIC 
// MAGIC vdata = corrCanadaTotal.selectExpr("Household_consumption", "Price as label")
// MAGIC 
// MAGIC stages = []
// MAGIC assembler = VectorAssembler(inputCols=["Household_consumption"], outputCol="features")
// MAGIC stages += [assembler]
// MAGIC pipeline = Pipeline(stages=stages)
// MAGIC pipelineModel = pipeline.fit(vdata)
// MAGIC dataset = pipelineModel.transform(vdata)
// MAGIC # Keep relevant columns
// MAGIC selectedcols = ["features", "label"]

// COMMAND ----------

// MAGIC %python
// MAGIC display(dataset.select(selectedcols))

// COMMAND ----------

// MAGIC %python
// MAGIC #build the model
// MAGIC # Import LinearRegression class
// MAGIC from pyspark.ml.regression import LinearRegression
// MAGIC 
// MAGIC # Define LinearRegression algorithm
// MAGIC lr = LinearRegression()
// MAGIC 
// MAGIC # Fit 2 models, using different regularization parameters
// MAGIC modelA = lr.fit(dataset, {lr.regParam:0.0})
// MAGIC modelB = lr.fit(dataset, {lr.regParam:100.0})

// COMMAND ----------

// MAGIC %python
// MAGIC ## Make predictions
// MAGIC predictionsA = modelA.transform(dataset)
// MAGIC display(predictionsA)

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.ml.evaluation import RegressionEvaluator
// MAGIC evaluator = RegressionEvaluator(metricName="rmse")
// MAGIC RMSE = evaluator.evaluate(predictionsA)
// MAGIC print("ModelA: Root Mean Squared Error = " + str(RMSE))
// MAGIC 
// MAGIC 
// MAGIC predictionsB = modelB.transform(dataset)
// MAGIC RMSE = evaluator.evaluate(predictionsB)
// MAGIC print("ModelB: Root Mean Squared Error = " + str(RMSE))

// COMMAND ----------

// MAGIC %python
// MAGIC # Import numpy, pandas, and ggplot
// MAGIC import numpy as np
// MAGIC from pandas import *
// MAGIC from ggplot import *
// MAGIC 
// MAGIC # Create Python DataFrame
// MAGIC consumption = dataset.rdd.map(lambda p: (p.features[0])).collect()
// MAGIC price = dataset.rdd.map(lambda p: (p.label)).collect()
// MAGIC predA = predictionsA.select("prediction").rdd.map(lambda r: r[0]).collect()
// MAGIC predB = predictionsB.select("prediction").rdd.map(lambda r: r[0]).collect()
// MAGIC 
// MAGIC # Create a pandas DataFrame
// MAGIC pydf = DataFrame({'consumption':consumption,'price':price,'predA':predA, 'predB':predB})
// MAGIC # Visualizing the Model
// MAGIC # Create scatter plot and two regression models (scaling exponential) using ggplot
// MAGIC p = ggplot(pydf, aes('consumption','price'))+\
// MAGIC geom_point(color='blue') +\
// MAGIC geom_line(pydf, aes('consumption','predA'), color='red') +\
// MAGIC geom_line(pydf, aes('consumption','predB'), color='green') +\
// MAGIC scale_x_log10() + scale_y_log10()
// MAGIC display(p)

// COMMAND ----------

// MAGIC %md
// MAGIC https://towardsdatascience.com/building-a-linear-regression-with-pyspark-and-mllib-d065c3ba246a
// MAGIC 
// MAGIC Based on the information provided, the goal is to come up with a model to predict household consumption  value of a given house price.

// COMMAND ----------

val corrCanadaTotal= spark.sql("select Year, Price, Household_consumption from joinCanadaTotal")
display(corrCanadaTotal)

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.ml.feature import VectorAssembler
// MAGIC vectorAssembler = VectorAssembler(inputCols = [ 'Price'], outputCol = 'features')
// MAGIC vhouse_df = vectorAssembler.transform(corrCanadaTotal)
// MAGIC vhouse_df = vhouse_df.select(['features', 'Household_consumption'])
// MAGIC vhouse_df.show(3)

// COMMAND ----------

// MAGIC %python
// MAGIC corrCanadaTotal.describe().toPandas().transpose()

// COMMAND ----------

// MAGIC %python
// MAGIC splits = vhouse_df.randomSplit([0.7, 0.3])
// MAGIC train_df = splits[0]
// MAGIC test_df = splits[1]

// COMMAND ----------

// MAGIC %md
// MAGIC linear regression

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC from pyspark.ml.regression import LinearRegression
// MAGIC lr = LinearRegression(featuresCol = 'features', labelCol='Household_consumption', maxIter=10, regParam=0.3, elasticNetParam=0.8)
// MAGIC lr_model = lr.fit(train_df)
// MAGIC print("Coefficients: " + str(lr_model.coefficients))
// MAGIC print("Intercept: " + str(lr_model.intercept))

// COMMAND ----------



// COMMAND ----------

// MAGIC %python
// MAGIC trainingSummary = lr_model.summary
// MAGIC print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
// MAGIC print("r2: %f" % trainingSummary.r2)

// COMMAND ----------

// MAGIC %python
// MAGIC train_df.describe().show()

// COMMAND ----------

// MAGIC %md
// MAGIC R squared at 0.84 indicates that in our model, approximate 84% of the variability in “Price” can be explained using the model.

// COMMAND ----------

// MAGIC %python
// MAGIC lr_predictions = lr_model.transform(test_df)
// MAGIC lr_predictions.select("prediction","Household_consumption","features").show(5)
// MAGIC from pyspark.ml.evaluation import RegressionEvaluator
// MAGIC lr_evaluator = RegressionEvaluator(predictionCol="prediction", \
// MAGIC                  labelCol="Household_consumption",metricName="r2")
// MAGIC print("R Squared (R2) on test data = %g" % lr_evaluator.evaluate(lr_predictions))

// COMMAND ----------

// MAGIC 
// MAGIC %python
// MAGIC test_result = lr_model.evaluate(test_df)
// MAGIC print("Root Mean Squared Error (RMSE) on test data = %g" % test_result.rootMeanSquaredError)

// COMMAND ----------

// MAGIC %python
// MAGIC print("numIterations: %d" % trainingSummary.totalIterations)
// MAGIC print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
// MAGIC trainingSummary.residuals.show()

// COMMAND ----------

// MAGIC %md
// MAGIC Using our Linear Regression model to make some predictions:

// COMMAND ----------

// MAGIC %python
// MAGIC predictions = lr_model.transform(test_df)
// MAGIC predictions.select("prediction","Household_consumption","features").show()

// COMMAND ----------

// MAGIC %md
// MAGIC Decision tree regression

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.ml.regression import DecisionTreeRegressor
// MAGIC dt = DecisionTreeRegressor(featuresCol ='features', labelCol = 'Household_consumption')
// MAGIC dt_model = dt.fit(train_df)
// MAGIC dt_predictions = dt_model.transform(test_df)
// MAGIC dt_evaluator = RegressionEvaluator(
// MAGIC     labelCol="Household_consumption", predictionCol="prediction", metricName="rmse")
// MAGIC rmse = dt_evaluator.evaluate(dt_predictions)
// MAGIC print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

// COMMAND ----------

// MAGIC %md
// MAGIC Gradient-boosted tree regression

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.ml.regression import GBTRegressor
// MAGIC gbt = GBTRegressor(featuresCol = 'features', labelCol = 'Household_consumption', maxIter=10)
// MAGIC gbt_model = gbt.fit(train_df)
// MAGIC gbt_predictions = gbt_model.transform(test_df)
// MAGIC gbt_predictions.select('prediction', 'Household_consumption', 'features').show(5)

// COMMAND ----------

// MAGIC %python
// MAGIC gbt_evaluator = RegressionEvaluator(
// MAGIC     labelCol="Household_consumption", predictionCol="prediction", metricName="rmse")
// MAGIC rmse = gbt_evaluator.evaluate(gbt_predictions)
// MAGIC print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

// COMMAND ----------


