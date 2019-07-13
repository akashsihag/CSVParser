import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

object EmployeeAnalytics {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder()
      .appName("Qualys")
      .master("local[*]")
      .getOrCreate()

    val emp = spark.read.format("csv").option("header", "true").load("resources/emp.csv")
    val dept = spark.read.format("csv").option("header", "true").load("resources/dept.csv")

    val df = emp.join(dept, Seq("deptno"), "left_outer")
    df.cache()
    df.show()

    // Write a query and compute average salary (sal) of employees distributed by location (loc). Output shouldn't show any locations which don't have any employees.
    val queryOneDF = df.groupBy("loc").agg(
      functions.count("ename").alias("empcnt"),
      (functions.sum("sal") / functions.count("sal")).alias("average_salary")
    )
    println("average salary (sal) of employees distributed by location (loc):")
    queryOneDF.filter(queryOneDF("empcnt") > 0).drop("empcnt").show()


    // Write a query and compute average salary (sal) of employees located in NEW YORK excluding PRESIDENT
    val queryTwoDF = df.filter(df("loc").equalTo("NEW YORK") && df("job").notEqual("PRESIDENT"))
      .groupBy("loc").agg(
      (functions.sum("sal") / functions.count("sal")).alias("average_salary")
    )
    println("average salary (sal) of employees located in NEW YORK excluding PRESIDENT:")
    queryTwoDF.show()


    // Write a query and compute average salary (sal) of four most recently hired employees
    import spark.sqlContext.implicits._
    val pattern = "yyyy-MM-dd"
    val queryThreeDF = df
      .withColumn("hiredatetimestamp", functions.unix_timestamp(df("hiredate"), pattern).cast("timestamp"))
      .orderBy($"hiredatetimestamp".desc).limit(4)
    println("average salary (sal) of four most recently hired employees:")
    queryThreeDF.show()


    // Write a query and compute minimum salary paid for different kinds of jobs in DALLAS
    val queryFourDF = df.filter(df("loc").equalTo("DALLAS"))
      .groupBy("job").agg(
      (functions.min("sal")).alias("minimum_salary:")
    )
    println("minimum salary paid for different kinds of jobs in DALLAS")
    queryFourDF.show()
  }


}
