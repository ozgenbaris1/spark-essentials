package part5lowlevel

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import scala.io.Source

object RDDs extends App {

  val spark = SparkSession
    .builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext // entrypoint for creating RDDs

  // 1 - parallelize an existing collection
  val numbers = 1 to 1000000
  val numbersRDD = sc.parallelize(numbers) // distributed colletion of Ints

  // 2 - reading from files
  case class StockValue(symbol: String, date: String, price: Double)

  def readStocks(filename: String) =
    Source
      .fromFile(filename)
      .getLines()
      .drop(1) // remove header
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stocksRDD =
    sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2b - reading from files
  val stocksRDD2 = sc
    .textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - read from a DF
  val stocksDF =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/stocks.csv")

  // DF -> DS -> RDD
  import spark.implicits._
  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd

  // RDD -> DF
  val numbersDF = numbersRDD.toDF("numbers") // you lose the type information

  // RDD -> DS
  val numbersDS =
    spark.createDataset(numbersRDD) // you get to keep type information

  // Transformations
  val microsoftRDD =
    stocksRDD.filter(_.symbol == "MSFT") // lazy transformation

  // count
  val microsoftCount = microsoftRDD.count() // eager ACTION

  // distinct
  val companyNamesRDD =
    stocksRDD.map(_.symbol).distinct() // lazy transformation

  // min and max
  implicit val stockOrdering: Ordering[StockValue] =
    Ordering.fromLessThan((sa: StockValue, sb: StockValue) =>
      sa.price < sb.price
    )
  val minMicrosoft = microsoftRDD.min() // action

  // reduce
  numbersRDD.reduce(_ + _)

  // grouping
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol)
  // ^^ very expensive

  // partitioning
  val repartitionedStocksRDD =
    stocksRDD.repartition(30) // new RDD of stock value

  repartitionedStocksRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")

  /*
    Repartitioning is EXPENSIVE. By definition, involves shuffling (moving data accross nodes)
    Best practice: partition EARLY, then process
    Size of a partition should be between 10-100 MB
   */

  // coalesce

  // will not necessarily involve shuffling
  val coalescedRDD = repartitionedStocksRDD.coalesce(15)

  coalescedRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks15")

  /** Exercises
    *
    *   1. Read the movies.json as an RDD 2. Show the distinct genres as an RDD.
    *      3. Select all the movies in the Drama genre with IMDB rating > 6 4.
    *      Show the average rating of movies by genre
    */

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  case class Movie(title: String, genre: String, rating: Double)

  val moviesRDD = moviesDF
    .select(
      col("Title").as("title"),
      col("Major_Genre").as("genre"),
      col("IMDB_Rating").as("rating")
    )
    .where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd

  val genresRDD = moviesRDD.map(_.genre).distinct()

  val goodDramasRDD =
    moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6.4)

  case class GenreAvgRating(genre: String, rating: Double)

  val avgRatingByGenreRDD = moviesRDD
    .groupBy(_.genre)
    .map({ case (genre, movies) =>
      GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
    })

  avgRatingByGenreRDD.toDF.show()
}
