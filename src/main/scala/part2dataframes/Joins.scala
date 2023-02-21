package part2dataframes

import org.apache.spark.sql.SparkSession

object Joins extends App {
  val spark =
    SparkSession
      .builder()
      .appName("Joins")
      .config("spark.master", "local")
      .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // inner joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")

  // outer joins
  // left outer => everything in the inner join + all the rows in the left table (guitaristsDF in the case below)
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")

  // right outer => everything in the inner join + all the rows in the right table
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")

  // full outer => everything in the inner join + all the rows in BOTH tables
  guitaristsDF.join(bandsDF, joinCondition, "outer")

  // semi-joins => everything in the left DF for which there is a row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")

  // anti-joins => everything in the left DF for which there is NO row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_anti")

  // options to handle ambigous column names
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  guitaristsBandsDF.drop(bandsDF.col("id"))

}
