package part3typesdatasets

import org.apache.spark.sql.{SparkSession, Encoders, Dataset}
import org.apache.spark.sql.functions._
object Datasets extends App {
  val spark = SparkSession
    .builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/numbers.csv")

  // convert a DF to a Dataset
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  // dataset of a complex type
  // 1 - define your case class
  case class Car(
      Name: String,
      Miles_per_Gallon: Option[Double], // nullable
      Cylinders: Long,
      Displacement: Double,
      Horsepower: Option[Long],
      Weight_in_lbs: Long,
      Acceleration: Double,
      Year: String,
      Origin: String
  )

  // 2 - read the DF from the file
  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  // 3 - define an encoder (by importing the implicits)
  import spark.implicits._
  val carsDF = readDF("cars.json")

  // 4 - convert dataframe to dataset
  val carsDS = carsDF.as[Car]

  // DS collection functions
  numbersDS.filter(_ < 100)

  // map, flatMap, fold, reduce, for comprehension ...
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())

  /** Exercises
    *   1. Count how many cars we have 2. Count how many POWERFUL cars we have
    *      (HP > 140) 3. Average HP for the entire dataset
    */

  val carsCount = carsDS.count
  println(carsCount)

  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count)

  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsCount)

  // Joins
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(
      id: Long,
      name: String,
      guitars: Seq[Long],
      band: Long
  )
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPLayerBandsDS: Dataset[(GuitarPlayer, Band)] =
    guitarPlayersDS.joinWith(
      bandsDS,
      guitarPlayersDS.col("band") === bandsDS.col("id"),
      "inner"
    )

  /** Exercise:
    *   1. join the guitarsDS and guitarPlayersDS in an outer join (hint: use
    *      array_contains)
    */

  guitarPlayersDS
    .joinWith(
      guitarsDS,
      array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")),
      "outer"
    )

  // grouping datasets
  val carsGroupedByOrigin = carsDS.groupByKey(_.Origin).count()

  // joins and groups are WIDE transformations, will involve SHUFFLE operations

}
