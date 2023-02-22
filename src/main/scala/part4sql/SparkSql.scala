package part4sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

object SparkSql extends App {
  val dir = "src/main/resources/warehouse"
  val db = "rtjvm"

  val spark = SparkSession
    .builder()
    .appName("Spark SQL Practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", dir)
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // regular DF API
  carsDF.select(col("Name")).where(col("Origin") === "USA")

  // use Spark SQL
  carsDF.createOrReplaceTempView("cars")

  val americanCarsDF = spark.sql("""
    select Name from cars where Origin = 'USA'
  """.stripMargin)

  // we can run any SQL statement
  spark.sql(s"create database $db")
  spark.sql(s"use $db")
  val databasesDF = spark.sql("show databases")

  // transfer tables from a DB to Spark tables
  def readTable(table: String) = spark.read
    .format("jdbc")
    .options(
      Map(
        "driver" -> "org.postgresql.Driver",
        "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
        "user" -> "docker",
        "password" -> "docker",
        "dbtable" -> s"public.$table"
      )
    )
    .load()

  def transferTables(tableNames: List[String]) = tableNames.foreach {
    tableName =>
      val tableDF = readTable(tableName)
      tableDF.createOrReplaceTempView(tableName)
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
  }

  transferTables(
    List(
      "employees",
      "departments",
      "titles",
      "dept_emp",
      "salaries",
      "dept_manager"
    )
  )

}
