package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {
  val spark = SparkSession
    .builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Dates
  val moviesWithReleaseDates = moviesDF
    .select(
      col("Title"),
      // conversion
      to_date(col("Release_date"), "dd-MMM-yy").as(
        "Actual_Release"
      )
    )

  moviesWithReleaseDates
    .withColumn("Today", current_date()) // today
    .withColumn("Right_Now", current_timestamp()) // this second
    .withColumn(
      "Movie_Age",
      datediff(col("Today"), col("Actual_Release")) / 365
    )

  val stocksDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/data/stocks.csv")

  val stocksDFWithDates =
    stocksDF.withColumn("actual_date", to_date(col("date"), "MMM d yyyy"))

  // Structures
  // 1. with col operators
  moviesDF
    .select(
      col("Title"),
      struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit")
    )
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))

  // 2. with expression strings
  moviesDF
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")

  // Arrays
  val moviesWithWords = moviesDF
    .select(col("Title"), split(col("Title"), " |,").as("Title_Words"))
  // ARRAY of Strings

  moviesWithWords
    .select(
      col("Title"),
      expr("Title_Words[0]"),
      size(col("Title_Words")),
      array_contains(col("Title_Words"), "Love")
    )

}
