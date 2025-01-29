package Pipeline

import common.{CheckIfFileExistsElseCopy, FolderCreator}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class RetailDataProcessor(spark: SparkSession, path: String) {
  Logger.getLogger("org").setLevel(Level.WARN)
  val logger = Logger.getLogger(getClass)
  val sourcePath = path
  val transactionSourceFileName = "transactions.csv"
  val transactionsItemsSourceFileName = "transactionItems.csv"
  val storesSourceFileName = "stores.csv"
  val customersSourceFileName = "customers.csv"
  val productsSourceFileName = "products.csv"
  val inventorySourceFileName = "inventory.csv"
  val promotionSourceFileName = "promotion.csv"
  val supplierSourceFileName = "supplier.csv"


  def fetchCurrentDate(): String = {
    val currentDate = LocalDate.now()
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val formattedDate = currentDate.format(formatter).toString()
    formattedDate
  }

  val curDate = fetchCurrentDate()

  val listOfFiles = Seq("transactions.csv", "transactionItems.csv", "inventory.csv", "stores.csv", "customers.csv", "promotion.csv", "supplier.csv", "products.csv")
  logger.info("Reading the data from source file " + sourcePath + "\\" + curDate)
  val fil = new FolderCreator(sourcePath)
  fil.createFolderIfNotExists()

  val checkForFiles = new CheckIfFileExistsElseCopy(path + "\\" + "source", listOfFiles, path + "\\" + curDate)
  checkForFiles.checkForFiles()


  def removeDuplicates(df: DataFrame, array: Seq[String]): sql.DataFrame = {
    val res_df = df.dropDuplicates(array)
    res_df
  }

  def windowFunctionWithPartition(df: sql.DataFrame, partitionColumn: String, aggregateFunction: String, orderByColumns: String, aggColumn: Seq[String]): sql.DataFrame = {

    val window = Window.partitionBy(partitionColumn).orderBy(orderByColumns)

    if (aggregateFunction.toLowerCase.equals("average")) {
      val df1 = df.withColumn("average", avg(col(aggColumn.mkString(","))).over(window))
      df1
    }


    else if (aggregateFunction.toLowerCase.equals("sum")) {
      val df1 = df.withColumn("sum", sum(col(aggColumn.mkString(","))).over(window))
      df1
    }
    else if (aggregateFunction.toLowerCase.equals("count")) {
      val df1 = df.withColumn("count", count(col(aggColumn.mkString(","))).over(window))
      df1
    }
    else if (aggregateFunction.toLowerCase.equals("median")) {
      val medianByCountry = df
        .groupBy("country")
        .agg(expr("percentile_approx(lead_time, 0.5)").as("median_lead_time"))
      medianByCountry
    }

    else if (aggregateFunction.toLowerCase.equals("mode")) {
      val modeDF = df.groupBy("size")
        .agg(count("size").alias("count"))
        .orderBy(col("count").desc)
        .limit(1) // Get the most frequent size
      modeDF.show()
      modeDF
    }

    else {
      df
    }

  }


  def createDf(spark: SparkSession, sourcePath: String, SourceFileName: String, schema: StructType, date: String) = {


    val fullPath = sourcePath + "\\" + curDate + "\\" + SourceFileName
    val resultDfName = spark.read
      .option("header", "true")
      .schema(schema)
      .csv(fullPath)
    resultDfName
  }

  private def addDateColumn(df: DataFrame, curDate: String): DataFrame = {
    val d = df.withColumn("file_received_date", lit(curDate))
    d
  }

  def splitDemograp(df: DataFrame): sql.DataFrame = {

    val df1 = df.withColumn("name", split(col("demographics"), "\\|").getItem(0))
      .withColumn("sex", trim(split(col("demographics"), "\\|").getItem(1)))
      .withColumn("age", split(col("demographics"), "\\|").getItem(2))
    val df2 = df1.drop("demographics")
    df2
  }

  def readTransactionData(): DataFrame = {
    val transactionSchema = StructType(Array(
      StructField("transaction_id", IntegerType, nullable = true),
      StructField("store_id", IntegerType, nullable = true),
      StructField("customer_id", IntegerType, nullable = true),
      StructField("transaction_date", DateType,nullable = true),
      StructField("total_amount", FloatType, nullable = true)
    ))
    val transactionDf = createDf(spark, sourcePath, transactionSourceFileName, transactionSchema, curDate)
    val df1 = addDateColumn(transactionDf, curDate)
    val d = removeDuplicates(df1, Seq("transaction_id", "store_id"))
    d.filter(col("total_amount").isNotNull || col("total_amount") > 0)
    d.withColumn("month", month(col("transaction_date")))
    d
  }


  def removeNullDiscountTransactions(): Unit = {
    val res = readTransactionData().filter(col("discount").isNotNull)
    res.show()


  }

  def readTransactionItemsData(): DataFrame = {
    val transactionsItemsSchema = StructType(Array(
      StructField("transaction_id", IntegerType, nullable = true),
      StructField("product_id", IntegerType, nullable = true),
      StructField("quantity", IntegerType, nullable = true),
      StructField("unit_price", FloatType, nullable = true),
      StructField("discount", FloatType, nullable = true)
    ))


    val transactionsItemsDf = createDf(spark, sourcePath, transactionsItemsSourceFileName, transactionsItemsSchema, curDate)
    val transactionsItemsDf1 = addDateColumn(transactionsItemsDf, curDate)
    val transactionItemsAvgPrice = windowFunctionWithPartition(transactionsItemsDf1, "product_id", "average", "product_id", Seq("unit_price"))
    val transactionItemsAvgPriceFin = transactionItemsAvgPrice
      .withColumn("final_price", when(col("unit_price").isNull, col("average")).otherwise(col("unit_price")))
      .drop("unit_price", "average")
      .withColumnRenamed("final_price", "unit_price")
    val res = transactionItemsAvgPriceFin.withColumn("high_discount_flag",
      when(col("discount") < 20.00, "L")
        .when(col("discount") >= 20.00, "H")
        .otherwise(col("discount").cast("string"))
    )
    res
  }

  def removeNullDiscountTransactionItems(): Unit = {
    val res = readTransactionItemsData().filter(col("discount").isNotNull)
    res.show()
  }

  def readStoresData(): DataFrame = {
    val storesSchema = StructType(Array(
      StructField("store_id", IntegerType, true),
      StructField("location", StringType, true),
      StructField("region", StringType, true),
      StructField("size", StringType, true),
      StructField("opening_date", DateType, true)
    ))
    val storesDf = createDf(spark, sourcePath, storesSourceFileName, storesSchema, curDate)
    val d = addDateColumn(storesDf, curDate)

    val storesDfFin = d.withColumn("new_region",
      when(lower(col("region")).equalTo("north-east") || lower(col("region")).equalTo("ne"), "Northeast")
        .otherwise(col("region")))
    storesDfFin.withColumn("full_location", concat_ws("_",col("region"),col("location")))
    storesDfFin
  }

  def readCustomersData(): DataFrame = {
    val customersSchema = StructType(Array(
      StructField("customer_id", IntegerType, true),
      StructField("signup_date", DateType, true),
      StructField("segment", StringType, true),
      StructField("demographics", StringType, true)
    ))

    val customersDf = createDf(spark, sourcePath, customersSourceFileName, customersSchema, curDate)
    val d = addDateColumn(customersDf, curDate)
    val customersDfFin = d.join(readTransactionData().filter(months_between(lit(current_date()), col("transaction_date"), true) <= 36), Seq("customer_id"), "inner")
      .select(col("customer_id"), col("signup_date"), col("segment"), col("demographics"), col("transaction_id"))
      .withColumn("year", year(col("signup_date")))

    customersDfFin

  }

  def readProductData(): DataFrame = {
    val productSchema = StructType(Array(
      StructField("product_id", IntegerType, true),
      StructField("product_name", StringType, true),
      StructField("category_id", IntegerType, true),
      StructField("base_price", FloatType, true),
      StructField("supplier_id", IntegerType, true)
    ))
    val productDf = createDf(spark, sourcePath, productsSourceFileName, productSchema, curDate)
    val res = addDateColumn(productDf, curDate)
      val res1 = res.withColumn("Price_range",when(col("base_price") <= 400.00, "Low")
        .when(col("base_price").between(399.99, 699.99), "Medium")
        .when(col("base_price") >= 700.00, "High")
        .otherwise(col("base_price")))
    res1

  }

  def readInventoryData(): DataFrame = {
    val inventorySchema = StructType(Array(
      StructField("store_id", IntegerType, true),
      StructField("product_id", IntegerType, true),
      StructField("date", DateType, true),
      StructField("quantity", IntegerType, true),
      StructField("reorder_point", IntegerType, true)
    ))
    val inventoryDf = createDf(spark, sourcePath, inventorySourceFileName, inventorySchema, curDate)
    val res = addDateColumn(inventoryDf, curDate)

     val res1 =  res.withColumn("quantity_gap", col("quantity") - col("reorder_point"))
      .withColumn("quantity_percent", round((col("quantity_gap") / col("quantity")) * 100))

     val res2 = res1.withColumn("critical_quantity_gap_flag",
      when(col("quantity_percent") < 50.0, "L")
        .when(col("quantity_percent") >= 50.0, "H")
        .otherwise(col("quantity_percent").cast("string")))

    res2
  }

  def readPromotionData(): DataFrame = {
    val promotionSchema = StructType(Array(
      StructField("promo_id", IntegerType, true),
      StructField("start_date", DateType, true),
      StructField("end_date", DateType, true),
      StructField("discount_type", StringType, true),
      StructField("product_id", IntegerType, true)
    ))
    val promotionDf = createDf(spark, sourcePath, promotionSourceFileName, promotionSchema, curDate)
    addDateColumn(promotionDf, curDate)
  }

  def readSupplierData(): DataFrame = {
    val supplierSchema = StructType(Array(
      StructField("supplier_id", IntegerType, true),
      StructField("lead_time", IntegerType, true),
      StructField("reliability_score", FloatType, true),
      StructField("country", StringType, true),
      StructField("store_id", IntegerType, true)
    ))
    val promotionDf = createDf(spark, sourcePath, supplierSourceFileName, supplierSchema, curDate)
    val s = addDateColumn(promotionDf, curDate)

    //find the median of lead_time on country
    s
  }

  def imputeReliablityScoreSupplier(): Unit = {
    val productDf = readProductData()
    val suppliersDF = readSupplierData()
    val joinedDF1 = suppliersDF.join(productDf, "supplier_id")
    val supplierProductCountDF = joinedDF1
      .groupBy("supplier_id")
      .agg(countDistinct("product_id").alias("product_count"))

    val weightedSumDF = joinedDF1
      .filter(col("reliability_score").isNotNull) // Only consider non-null reliability scores
      .groupBy("supplier_id")
      .agg(
        avg(col("reliability_score")).alias("weighted_sum"),
        sum("product_id").alias("total_products")) // Total number of products for the supplier

    val joinedDF = supplierProductCountDF
      .join(weightedSumDF, "supplier_id")
      .withColumn(
        "weighted_avg_reliability_score",
        col("weighted_sum") / col("product_count") // Calculate weighted average
      )
    val avgWeightedReliabilityScore = joinedDF.agg(avg("weighted_avg_reliability_score")).collect()(0)(0)

    joinedDF.show()
    val resultDF = suppliersDF.join(joinedDF.select("supplier_id", "weighted_avg_reliability_score"), Seq("supplier_id"), "left")
      .withColumn(
        "imputed_reliability_score",
        when(col("reliability_score").isNull, avgWeightedReliabilityScore)
          .otherwise(col("reliability_score"))
      )
      .drop("weighted_avg_reliability_score") // Drop the temporary column
    resultDF.show()

    //    val res1 = readSupplierData().join(readProductData(),"supplier_id")
    //    val res2 = res1.groupBy(col("supplier_id")).agg(count(col("product_id")).alias("count"),sum(col("reliability_score")).alias("total_reliability_score"))
    //    val res3 = res2.withColumn("avg",col("total_reliability_score")/col("count"))
    //    res3.drop("total_reliability_score","count")
    //    res3.show()

  }


  //find the median of lead_time on country
  def calculateMedianCountry(): Unit = {
    val s1 = windowFunctionWithPartition(readSupplierData, "country", "median", "lead_time", Seq("lead_time"))
    s1.show()
  }

  def calculateAverageRegionFillSize(): Unit = {
    val s = windowFunctionWithPartition(readStoresData(), "region", "mode", "size", Seq("lead_time"))

    val size = s.collect()(0)(0)
    val newDf = readStoresData.withColumn("new_size", when(col("size").isNull, size).otherwise(col("size")))
      .drop(col("size"))
      .withColumnRenamed("new_size", "size")
    newDf.show()
    newDf
  }

  def replaceDemographicsWithUnknown(): DataFrame = {

    val newdf = readCustomersData().withColumn("new_d", when(col("demographics").isNull, "unknown").otherwise(col("demographics")))
      .drop(col("demographics"))
      .withColumnRenamed("new_d", "demographics")

    val d1 = splitDemograp(newdf)
    d1.show()
    d1

  }


  def joinThreeTables(df1: DataFrame, df2: DataFrame, df3: DataFrame, colName1: String, colName2: String, typeJoin: String): sql.DataFrame = {
    val joinedDf = df1.join(df2, Seq(colName1), typeJoin).join(df3, Seq(colName2), typeJoin)
    joinedDf
  }

  def calculateRevenue(df1: DataFrame, df2: DataFrame, df3: DataFrame, colName1: String, colName2: String, typeJoin: String): Unit = {
    val joinedDf = joinThreeTables(df1, df2, df3, colName1, colName2, "inner")
    val revenueDF = joinedDf.withColumn("revenue", col("quantity") * col("unit_price"))
    val totalRevenueByCategory = revenueDF
      .groupBy("category_id")
      .agg(sum("revenue").alias("total_revenue"))
    val totalRevenue = totalRevenueByCategory
      .agg(sum("total_revenue").alias("total_revenue"))
      .collect()(0)(0)
    val revenueShareDF = totalRevenueByCategory
      .withColumn("revenue_share", col("total_revenue") / totalRevenue * 100)
    //revenueShareDF

  }

  def fetchCustomerNameWhoPurchasedProducts(df1: DataFrame, df2: DataFrame, df3: DataFrame, colName1: String, colName2: String, typeJoin: String): Unit = {
    val joinedDf2 = joinThreeTables(df1, df2, df3, colName1, colName2, typeJoin)
    joinedDf2.join(replaceDemographicsWithUnknown, Seq("customer_id"), "inner")
      .filter(col("promo_id").isNotNull)
      .select(col("name"))
  }


  //Find suppliers with the highest reliability scores who supply to stores in a specific region by
  //joining suppliers, products, and stores
  def findHighestReliabilityScore(df1: DataFrame, df2: DataFrame, df3: DataFrame, colName1: String, colName2: String, typeJoin: String): Unit = {
    val joinedDf3 = joinThreeTables(df1, df2, df3, colName1, colName2, "inner")
    val res = joinedDf3.groupBy(col("new_region"), col("supplier_id")).agg(max(col("reliability_score")).alias("max_reliability_score"))
    //res.show(20,false)

  }


  def findStocksOut(df1: DataFrame, df2: DataFrame, colName1: String, typeJoin: String): Unit = {
    val res = df1.join(df2, df1(colName1) === df2(colName1), typeJoin)
    val joinedDF = res.withColumn("stock_out", when(col("quantity") <= col("reorder_point"), lit(true)).otherwise(lit(false)))
    // Show the result where stock_out is true
    joinedDF.filter(col("stock_out") === true).show()
  }


  def findTotalRevenuePerDemographic(df1: DataFrame, df2: DataFrame, colName1: String, typeJoin: String): Unit = {
    val res = df1.join(df2, df1(colName1) === df2(colName1), typeJoin)
      .groupBy("segment")
      .agg(sum("total_amount").as("total_revenue"))
      .orderBy(desc("total_revenue")) // Sort by total_revenue in descending order

    res.show()
  }


}
