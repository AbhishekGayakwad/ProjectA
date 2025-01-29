package Pipeline

import Validation.DataValidator
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._



object
MasterPipeline {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    val logger = Logger.getLogger(getClass)

    logger.info("Initializing Spark Session")
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("ProjectA")
      .getOrCreate()

    //val date = args(0)

    val sourcePath = "C:\\Users\\Admin\\Desktop\\Restart\\ProjectA_Dataset"


    val processor = new RetailDataProcessor(spark, sourcePath)
    val validate = new DataValidator(spark)


    //load all the datasets to dataframe for further processing

    val transactionDf = processor.readTransactionData().cache()
    val transactionItemDf = processor.readTransactionItemsData().cache()
    val supplierDf = processor.readSupplierData().cache()
    val customerDf = processor.readCustomersData().cache()
    val storesDf = processor.readStoresData().cache()
    val inventoryDf = processor.readInventoryData().cache()
    val productsDf = processor.readProductData().cache()
    val promotionDf = processor.readPromotionData().cache()



    //validate questions

    //Question 1
    validate.validateTransactionDf(transactionItemDf, transactionDf, "transaction_id", "transaction_id", "left_anti")
    //Question 2 and Question 3
    validate.validateInventoryDf(inventoryDf, storesDf, "store_id", "store_id", "left_outer")
    //Question 4
    validate.validatePromotionDf(promotionDf, productsDf, "product_id", "product_id", "left_outer")
    //Question 5
    val storesDfFin = validate.validateStoresDf(storesDf, "size")


    //Complex joins
    //question1
    processor.calculateRevenue(transactionDf, transactionItemDf, productsDf, "transaction_id", "product_id", "inner")
    //question2
     processor.fetchCustomerNameWhoPurchasedProducts(transactionDf, transactionItemDf, promotionDf, "transaction_id", "product_id", "left")
    //question3
     processor.findHighestReliabilityScore(supplierDf, productsDf, storesDf, "supplier_id", "store_id", "inner")
    //question4
    processor.findStocksOut(inventoryDf, storesDfFin, "store_id", "inner")
    //question5
    processor.findTotalRevenuePerDemographic(customerDf, transactionDf, "customer_id", "inner")

    //Null Handling
    //Question1
    processor.calculateMedianCountry()
    //question2
    processor.removeNullDiscountTransactionItems()
    //Question3
    processor.calculateAverageRegionFillSize()
    //Question4
    processor.replaceDemographicsWithUnknown()
    //Question5
    processor.imputeReliablityScoreSupplier()


    //string Manipulation
    //question1
    processor.readCustomersData()
    //question2
    val prDf = processor.readProductData()
    prDf.filter(col("product_name").like("%Eco%"))
    prDf.show()
    //question3
    processor.readStoresData().withColumn("uppercase_size", upper(col("size")))
      .drop("size")
      .withColumnRenamed("uppercase_size", "size").show()

    //question4
    processor.readStoresData().withColumn("trim_location", trim(col("location")))
      .drop("location")
      .withColumnRenamed("trim_location", "location").show()

    //question5
    processor.splitDemograp(processor.readCustomersData()).show()


    //Optimization
    //question1
    val res = processor.readStoresData().join(broadcast(processor.readTransactionData()), "store_id")
    res.show()
    //question2
    val resDf = processor.readTransactionItemsData().repartition(col("product_id"))
    resDf.show()
    //question3
    // caching of the dataframe done in above lines
    //question4

    //question5
    val win = Window.partitionBy("store_id")
      .orderBy("transaction_date")
      .rowsBetween(-6, 0)

    val resultDF = transactionDf
      .withColumn("7_day_rolling_avg", avg("total_amount").over(win))


    //Advanced Calculations
    //Q1
    val retentionDateDf = transactionDf.join(customerDf, "customer_id").filter(col("transaction_date") >= col("signup_date"))
    retentionDateDf.cache()
    val totalCustomers = customerDf.count()
    val activeCustomers = retentionDateDf.select("customer_id").distinct().count()
    val retentionRate = activeCustomers.toDouble / totalCustomers.toDouble
    println(s"Customer Retention Rate: ${retentionRate * 100}%")
    //q2

    val joinedProductTransactionItemDf = productsDf.join(transactionItemDf, "product_id")
    val joinedProductTransactionItemTransactionDf = joinedProductTransactionItemDf.join(transactionDf, "transaction_id")
    val joinedProductTransactionItemDfYearMonth = joinedProductTransactionItemTransactionDf.withColumn("year_month", date_format(col("transaction_date"), "yyyy-MM"))
    val res12 = joinedProductTransactionItemDfYearMonth.groupBy(col("product_id"), col("year_month")).agg(sum(col("total_amount")).alias("monthly_sales"))
    res12.show()
    val windowSpec1 = Window.partitionBy("product_id").orderBy("year_month")
    val salesWithGrowthDF = res12.withColumn("previous_month_sales", lag("monthly_sales", 1).over(windowSpec1))
      .withColumn("sales_growth", (col("monthly_sales") - col("previous_month_sales")) / col("previous_month_sales") * 100)
    val last6MonthsDF = salesWithGrowthDF.filter(col("year_month") >= "2025-01") // Adjust based on the date range

    // Check if all months have positive sales growth for each product
    val consistentGrowthDF = last6MonthsDF.groupBy("product_id")
      .agg(count(when(col("sales_growth") > 0, 1)).alias("positive_growth_count"),
        count("sales_growth").alias("total_months")
      )
      .filter(col("positive_growth_count") === col("total_months")) // Products with consistent growth

    consistentGrowthDF.show()


    //Q3

    //Q4

    val rankWindow = Window.partitionBy(col("country")).orderBy(col("reliability_score"))
    val rankSupplier = supplierDf.withColumn("rank", rank().over(rankWindow))
    rankSupplier.show()

    //Calculate average lead_time for products grouped by category in products to determine supply chain efficiency

    val joinProductSupplier = productsDf.join(supplierDf, "supplier_id")

    val groupByCategoryDf = joinProductSupplier.groupBy(col("category_id")).agg(avg("lead_time"))
    groupByCategoryDf.show()


    //Final Outputs and Reports
    //Q1
    val rs12 = storesDf.join(transactionDf, "store_id")
    val weekYearDf = rs12.withColumn("week_of_year", weekofyear(col("transaction_date")))
    val groupResult = weekYearDf.groupBy(col("store_id"), col("week_of_year")).agg(sum("total_amount"))
    groupResult.show()

    //Q2
    //Create a summary of products with the highest discounts and their sales impact, joining
    // products and transaction_items and calculating total revenue

    val rs13 = productsDf.join(transactionItemDf.filter(col("unit_price").isNotNull), "product_id")
      .withColumn("total_price", col("quantity") * col("unit_price"))
      .withColumn("discounted_price", col("total_price") - ((col("discount") / 100) * col("unit_price")) * col("quantity"))
      .dropDuplicates()
    rs13.show()
    val maxDiscount = rs13.groupBy("product_id").agg(max(col("discount"))).collect()(0)(1)
    val productWithMaxDiscount = rs13.filter(col("discount").equalTo(maxDiscount)).
      groupBy(col("product_id"))
      .agg(sum("total_price"), sum("discounted_price"))
    productWithMaxDiscount.show()


    //Q3
    val joinedTransactionDF = transactionDf.join(transactionItemDf, "transaction_id")
    val joinedDF = joinedTransactionDF
      .join(promotionDf, joinedTransactionDF("product_id") === promotionDf("product_id")
        && joinedTransactionDF("transaction_date").between(promotionDf("start_date"), promotionDf("end_date")), "left")
      .withColumn("is_promoted", when(col("promo_id").isNotNull, lit(1)).otherwise(lit(0)))


    val revenueDF = joinedDF.groupBy("is_promoted")
      .agg(sum("total_amount").alias("total_revenue"),
        count("transaction_id").alias("transaction_count"))

    revenueDF.show()

    val revenueStats = revenueDF.collect()

    if (revenueStats.length < 2) {
      val promotedRevenue = 1.00
      val nonPromotedRevenue = 1.00
    }
    else {
      try {
        val promotedRevenue = revenueStats(0).getAs[Double]("total_revenue") // Revenue from promotions
        val nonPromotedRevenue = revenueStats(1).getAs[Double]("total_revenue") // Revenue from non-promotions
        val uplift = (promotedRevenue - nonPromotedRevenue) / nonPromotedRevenue * 100
        println(s"Revenue uplift during promotion: $uplift%")
      }
      catch {
        case e: ArrayIndexOutOfBoundsException =>
          println("Caught an ArrayIndexOutOfBoundsException: " + e.getMessage)
      }
      finally {
        // Optional block to clean up resources, always runs
        println("This block runs no matter what.")
      }
    }

    //Q4
    val res123 = storesDf.join(transactionDf, "store_id")
      .join(supplierDf, "store_id")
      .join(productsDf, "supplier_id")
    val res12345 = res123.groupBy(col("store_id"))
      .agg(sum(col("total_amount")).alias("sum_total_amount"), countDistinct(col("product_id")).alias("total_distinct_count"))
      .orderBy(col("sum_total_amount").desc, col("total_distinct_count").desc)
      .limit(5)

    res12345.show(100)

    //Q5
    val joinedStoreTransactionDF = transactionDf.join(storesDf, "store_id").join(transactionItemDf, "transaction_id")
    val revenueDf1 = joinedStoreTransactionDF
      .groupBy(col("region"))
      .agg(sum(col("total_amount").alias("total_revenue")))

    val avgDiscountDF = joinedStoreTransactionDF
      .groupBy(col("region"))
      .agg(avg(col("discount")).alias("average_discount"))

    val topProductsDF = joinedStoreTransactionDF
      .groupBy(col("region"), col("product_id"))
      .agg(count(col("transaction_id")).alias("product_sales"))
      .join(productsDf, "product_id")
      .groupBy("region")
      .agg(first("product_name").alias("top_product"), sum("product_sales").alias("total_sales"))
      .orderBy(col("total_sales").desc)

    val dashboardDF = revenueDf1
      .join(avgDiscountDF, "region")
      .join(topProductsDF, "region")

    dashboardDF.show()

  }

}
