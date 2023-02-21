package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max, col}

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

  /** Exercises
    *
    *   - show all employees and their max salary
    *   - show all employees who were never manager
    *   - find the job titles of the best paid 10 employees in the company
    */

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

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")

  val maxSalariesPerEmployeeDF =
    salariesDF.groupBy(col("emp_no")).agg(max("salary").as("salary"))

  val employeeSalaryDF = employeesDF.join(maxSalariesPerEmployeeDF, "emp_no")

  val managersDF = readTable("dept_manager")

  val nonManagersDF = employeesDF.join(
    managersDF,
    managersDF.col("emp_no") === employeesDF.col("emp_no"),
    "left_anti"
  )

  val titlesDF = readTable("titles")

  val mostRecentJobTitlesDF =
    titlesDF.groupBy("emp_no", "title").agg(max("to_date"))

  val topPaidEmployees = employeeSalaryDF.orderBy(col("salary").desc).limit(10)

  val topPaidTitles = topPaidEmployees.join(mostRecentJobTitlesDF, "emp_no")

  topPaidTitles.show()

}
