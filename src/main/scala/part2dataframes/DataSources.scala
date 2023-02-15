package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.DateType

object DataSources extends App {
  val spark = SparkSession
    .builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  // schema
  val carsSchema = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", DateType),
      StructField("Origin", StringType)
    )
  )

  /** Reading a DF:
    *   - format
    *   - schema (optional) (or inferSchema = true)
    *   - zero or more options
    */

  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .option(
      "mode",
      "failFast" // if we encounter malformed record, spark will throw exception
    ) // other options: dropMalformed, permissive (default)
    .option(
      "path",
      "src/main/resources/data/cars.json"
    ) // or -> load("src/main...")
    .load()

  // alternative reading with options map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(
      Map(
        "mode" -> "failFast",
        "path" -> "src/main/resources/data/cars.json",
        "inferSchema" -> "true"
      )
    )
    .load()

  /** Writing DFs:
    *   - format
    *   - save mode = overwrite, append, ignore, errorIfExists
    *   - path
    *   - zero or more options
    */

  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path", "src/main/resources/data/cars_dup.json")
    .save()

  // JSON flags
  spark.read
    .schema(carsSchema)
    .options(
      Map(
        "dateFormat" -> "YYYY-MM-dd", // couple with schema; if Spark fails parsing, it will put null
        "allowSingleQuotes" -> "true",
        "compression" -> "uncompressed" // bzip2, gzip, lz4, snappy, deflate
      )
    )
    .json("src/main/resources/data/cars.json")

  // CSV flags
  val stocksSchema = StructType(
    Array(
      StructField("symbol", StringType),
      StructField("date", DateType),
      StructField("price", DoubleType)
    )
  )

  val stocks = spark.read
    .schema(stocksSchema)
    .options(
      Map(
        "dateFormat" -> "MMM dd YYYY",
        "header" -> "true",
        "sep" -> ",", // default: ','
        "nullValue" -> ""
      )
    )
    .csv("src/main/resources/data/stocks.csv")

  // Parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/cars.parquet")

  // Text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()
}
