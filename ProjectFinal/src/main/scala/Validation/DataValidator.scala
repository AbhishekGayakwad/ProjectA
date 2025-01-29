package Validation

import org.apache.spark.sql.functions.{col, lit, lower, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataValidator(spark: SparkSession) {

  def validateIdPresentInBothDf(df1: DataFrame, df2: DataFrame, df1Id: String, df2Id: String, joinCondition: String): DataFrame = {
    val res = df1.join(df2, df1(df1Id) === df2(df2Id), joinCondition)
    res
  }

  def checkColumnLessThen(df: DataFrame, columnToCheck: String, newColName: String, value: String): DataFrame = {
    val resDf = df.withColumn(newColName, when(col(columnToCheck) < 0, value).otherwise("invalid"))
    resDf
  }

  def checkValuesInColumn(df: DataFrame, column: String): DataFrame = {
    val predefinedValues = Seq("Small", "Large", "Medium", "S", "L", "M")
    val res = df.filter(col(column).isin(predefinedValues: _*))
    val res1 = res.withColumn("new_col",
        when(lower(col(column)).equalTo("small"), "Small")
          .when(lower(col(column)).equalTo("s"), "Small")
          .when(lower(col(column)).equalTo("medium"), "Medium")
          .when(lower(col(column)).equalTo("m"), "Medium")
          .when(lower(col(column)).equalTo("large"), "Large")
          .when(lower(col(column)).equalTo("l"), "Large")
          .otherwise(col(column)))
      .drop(col("size"))
      .withColumnRenamed("new_col", "size")
    res1
  }

  def validateTransactionDf(df1: DataFrame, df2: DataFrame, col1: String, col2: String, joinType: String): Unit = {
    val flag = validateIdPresentInBothDf(df1, df2, col1, col2, joinType)
    if (flag.count() == 0) {
      println("File passed validation")
    }
    else {
      println("File has data issue. PLease check")
      throw new IllegalArgumentException("Data validation failed: few transaction_id present in  transaction_items are not present in transactions")
    }

  }

  def validateInventoryDf(df1: DataFrame, df2: DataFrame, col1: String, col2: String, joinType: String): Unit = {
    val inventoryFlagDf = validateIdPresentInBothDf(df1, df2, col1, col2, joinType).filter(df2(col2).isNull)
    val checkColumnLessThenOutput = checkColumnLessThen(df1, "quantity", "valid", "valid").filter(col("valid").equalTo("invalid")).count()
    if (inventoryFlagDf.count() > 0) { //  || checkColumnLessThenOutput >0  --> this condition removed just process further logic
      println("File has data issue. PLease check ")
      throw new IllegalArgumentException("Data validation failed: few transaction_id present in  transaction_items are not present in transactions")
    }
    else {
      println("File passed validation")

    }
  }

  def validatePromotionDf(df1: DataFrame, df2: DataFrame, col1: String, col2: String, joinType: String): Unit = {
    val removeInvalidDataPromo1 = validateIdPresentInBothDf(df1, df2, col1, col2, joinType)
      .withColumn("is_valid", when(df2("product_id").isNull, lit(false)).otherwise(lit(true)))
      .filter(col("is_valid").equalTo(true)).filter(df2("product_id").equalTo(false))
    if (removeInvalidDataPromo1.count() > 0) {
      println("File has data issue. PLease check ")
      throw new IllegalArgumentException("Data validation failed: few transaction_id present in  transaction_items are not present in transactions")
    }
    else {
      println("File passed validation")

    }
  }

  def validateStoresDf(df1: DataFrame, colName: String): DataFrame = {
    val check5 = checkValuesInColumn(df1, colName)
    check5
  }

}
