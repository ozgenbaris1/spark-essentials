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

  allCountriesDF.show()

}
