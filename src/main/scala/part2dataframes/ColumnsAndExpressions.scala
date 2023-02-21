package part2dataframes

import org.apache.spark.sql.SparkSession

object ColumnsAndExpressions extends App {
  val spark = SparkSession
    .builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Columns
  val firstColumn = carsDF.col("Name") // a JVM object with no data inside

  // selecting (projection)
  val carNamesDF =
    carsDF.select(
      firstColumn
    ) // a new dataframe with single column, because dataframes are immutable

  // various select methods
  import org.apache.spark.sql.functions.{col, column, expr}
  import spark.implicits._

  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala Symbol, auto-converted to column
    $"Horsepower", // returns a column object
    expr("Origin") // EXPRESSION
  )

  // select with plain column names
  carsDF.select("Name", "Year")

  // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = simplestExpression / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  // selectExpr
  val carsWithSelectExprWeightsDF =
    carsDF.selectExpr("Name", "Weight_in_lbs", "Weight_in_lbs / 2.2")

  // DF processing

  // adding a column
  val carsWithKg3DF =
    carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)

  // renaming a column
  val carsWithColumnRenamed =
    carsDF.withColumnRenamed("Weight_in_lbs", "Weight_in_pounds")

  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering
  val europeanCarsDF =
    carsDF.filter(col("Origin") =!= "USA") // =!= is not equal operator

  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")

  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")

  // chain filters
  val americanPowerfulCarsDF =
    carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)

  val americanPowerfulCarsDF2 =
    carsDF.filter((col("Origin") === "USA") and (col("Horsepower") > 150))

  val americanPowerfulCarsDF3 =
    carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // union
  val moreCarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/more_cars.json")

  val allCarsDF =
    carsDF.union(moreCarsDF) // works if the DFs have the same schema

  // distinct
  val allCountriesDF = carsDF.select("Origin").distinct()

  // allCountriesDF.show()

  /** Exercises
    *   1. Read the movies DF and select 2 columns of your choice 2. Create
    *      another column summing up the total profit of the movies ? US_Gross +
    *      Worldwide_Gross + US_DVD_sales 3. Select all COMEDY movies with IMDB
    *      rating above 6
    */

  // Exercise 1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesDF1 = moviesDF.select(col("Title"), col("Release_Date"))

  // Exercise 2
  val moviesDFWithTotalProfit = moviesDF
    .withColumn(
      "Total_Profit",
      col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")
    )

  // Exercise 3
  val comedyMoviesWith6RatingDF =
    moviesDF.where(
      (col("Major_Genre") === "Comedy") and (col("IMDB_Rating") > 6)
    )
}
