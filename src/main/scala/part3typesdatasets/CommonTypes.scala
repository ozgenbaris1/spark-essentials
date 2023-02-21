package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App {
  val spark = SparkSession
    .builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // adding a plain value to a DF
  moviesDF.select(col("Title"), lit(47).as("plain_value"))

  // booleans
  val dramaFilter = col("Major_Genre") === "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val prefferedFilter = dramaFilter and goodRatingFilter

  val moviesWithGoodDramaFlagDF =
    moviesDF.select(col("Title"), prefferedFilter.as("is_good_drama"))

  val goodDramaMoviesDF = moviesWithGoodDramaFlagDF.where("is_good_drama")

  val notGoodDramaMoviesDF =
    moviesWithGoodDramaFlagDF.where(not(col("is_good_drama")))

  // numbers
  val moviesAvgRatingsDF =
    moviesDF.select(
      col("Title"),
      ((col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)
        .as("normalized_rating")
    )

  // correlation between rotten tomatoes rating and imdb rating
  println(
    moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")
  ) /* corr is an ACTION */

  // strings
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // capitalization (initcap, lower, upper)
  carsDF.select(col("Name"), initcap(col("Name")))

  carsDF.select("*").where(col("Name").contains("volkswagen"))

  // regex
  val regexString = "volkswagen | vw"

  val vwDF = carsDF
    .select(
      col("Name"),
      regexp_extract(col("Name"), regexString, 0).as("regex_extract")
    )
    .where(col("regex_extract") =!= "")
    .drop("regex_extract")

  /** Exercise
    *
    * Filter the cars DF by a list of car names obtianed by an API call
    * Versions:
    *   - regexes
    *   - contains
    */

  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  val carNames = getCarNames

  val carRegex = carNames.mkString(" | ").toLowerCase()

  val regexFilteredCarsDF = carsDF
    .select(
      col("*"),
      regexp_extract(col("Name"), carRegex, 0).as("regex_extract")
    )
    .where(col("regex_extract") =!= "")
    .drop("regex_extract")

  val carNameFilters =
    getCarNames
      .map(x => x.toLowerCase())
      .map(name => col("Name").contains(name))

  val bigFilter =
    carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) =>
      combinedFilter or newCarNameFilter
    )

  carsDF.filter(bigFilter).show()

}
