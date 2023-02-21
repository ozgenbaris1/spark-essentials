import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{
  count,
  col,
  countDistinct,
  approx_count_distinct,
  min,
  sum,
  avg,
  mean,
  stddev
}
object Aggregations extends App {
  val spark = SparkSession
    .builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // countin
  // all the genre values except null
  val genresCountDF = moviesDF.select(count(col("Major_Genre")))

  // count all the rows, and include nulls
  val numberOfRowsDF =
    moviesDF.select(count("*"))

  // count distinct values
  val distinctGenresDF = moviesDF.select(countDistinct(col("Major_Genre")))

  // approximate count (useful for quick data analysis)
  val approximateCountDistinctDF =
    moviesDF.select(approx_count_distinct(col("Major_Genre")))

  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))

  val sumUSGrossDF = moviesDF.select(sum(col("US_Gross")))

  val avgRottenTomatoesRatingDF =
    moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))

  // data science related
  val statsDF = moviesDF
    .select(
      mean(col("Rotten_Tomatoes_Rating")),
      stddev(col("Rotten_Tomatoes_Rating"))
    )

  // grouping

  // select count(*) from movies group by Major_Genre
  val countByGenreDF = moviesDF.groupBy(col("Major_Genre")).count()

  val avgRatingByGenre = moviesDF.groupBy(col("Major_Genre")).avg("IMDB_Rating")

  val aggregationsByGenreDF =
    moviesDF
      .groupBy(col("Major_Genre"))
      .agg(count("*").as("N_Movies"), avg("IMDB_Rating").as("Avg_Rating"))
      .orderBy(col("Avg_Rating"))

  /** Exercises
    *
    *   1. Sum up all the profits of all the movies in the DF 2. Count how many
    *      distinct directors we have 3. Show the mean and standard deviation of
    *      US gross revenue for the movies 4. Compute the average IMDB rating
    *      and the average US gross revenue per director
    */

  val sumOfProfitsDF =
    moviesDF
      .select(
        (col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales"))
          .as("Total_Gross")
      )
      .select(sum(col("Total_Gross")))

  val distinctDirectorsDF = moviesDF.select(countDistinct(col("Director")))

  val meanAndStdevOfUSGrossDF = moviesDF.select(
    mean(col("US_Gross")),
    stddev(col("US_Gross"))
  )

  val aggregationsByDirectorDF = moviesDF
    .groupBy(col("Director"))
    .agg(
      avg(col("IMDB_Rating")).as("Avg_Rating"),
      avg(col("US_Gross")).as("Total_US_Gross")
    )
    .orderBy(col("Avg_Rating").desc_nulls_last)

  aggregationsByDirectorDF.show()

}
