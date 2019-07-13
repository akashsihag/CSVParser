import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object Transform {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    println()
    if (args.length != 1) {
      println("Please enter file name!")
      System.exit(0)
    }
    val spark = SparkSession.builder()
      .appName("Qualys")
      .master("local[*]")
      .getOrCreate()

    val emp = spark.sparkContext.textFile(args(0))
    val headerAndTestData = emp.take(2).zipWithIndex
    val header = headerAndTestData.filter(x => x._2 == 0).map(x => x._1)
    val testData = headerAndTestData.filter(x => x._2 == 1).map(x => x._1)

    val empRDD = emp.mapPartitionsWithIndex { (index, itr) =>
      if (index == 0) {
        itr.drop(1).drop(1)
      }
      itr
    }

    def dataTypeExtractor(ele: String): DataType = {
      val integerPattern = "[0-9]*"
      val doublePattern = "[0-9]+[.]?[0-9]*"
      var dataTypeColumn: DataType = null

      val elem = ele.replaceAll("\"|\"", "")
      if (integerPattern.r.pattern.matcher(elem).matches()) {
        dataTypeColumn = IntegerType
      }
      else if (doublePattern.r.pattern.matcher(elem).matches()) {
        dataTypeColumn = DoubleType
      }
      else {
        dataTypeColumn = StringType
      }
      dataTypeColumn
    }

    def processLog(str: String, header: Array[String], dataType: Array[DataType]): Row = {
      val regex = """"((?:[^"\\]|\\[\\"ntbrf])+)"""".r
      var errorMessage = "NA"
      var corruptColumns: List[String] = List[String]()

      val strSplitted = str.split(",")
      val itr = strSplitted.flatMap { ele =>
        if (ele.trim != "") {
          regex.findAllIn(ele)
        }
        else
          Iterator("")
      }

      val strCleaned = itr.map(_.replaceAll("\"|\"", "").trim)

      if (strCleaned.length != header.length) {
        errorMessage = "\"The number of columns in the record doesn't match file header spec.\""
        return Row.fromSeq(errorMessage +: strSplitted)
      }
      else {
        for (i <- 0 until (strCleaned.length)) {
          if (dataTypeExtractor(strCleaned(i)) != dataType(i)) {
            if (dataType(i) == DoubleType & dataTypeExtractor(strCleaned(i)) == IntegerType) {
            }
            else if (dataType(i) == IntegerType & dataTypeExtractor(strCleaned(i)) == DoubleType) {
            }
            else {
              corruptColumns = header(i) :: corruptColumns
            }
          }
        }

        if (corruptColumns.length > 0) {
          errorMessage = "\"The datatypes of columns: [" + corruptColumns.mkString(", ") + "] doesn't match the datatypes specified in the first test record.\""
          return Row.fromSeq(errorMessage +: strSplitted)
        }
        else {
          return Row.fromSeq(errorMessage +: strCleaned)
        }
      }
    }

    val headerProcessed = header(0).split(",").map(_.replaceAll("\"|\"", "").trim)
//    println(headerProcessed.mkString(","))

    val testDataType = testData(0).split(",").map(dataTypeExtractor(_))
//    println(testDataType.mkString(","))

    val empProcessed = empRDD.map(record => processLog(record, headerProcessed, testDataType))

    def createSchema(header: Array[String], dataType: Array[DataType]): StructType = {
      val empStruct: Array[StructField] = new Array[StructField](header.length)
      for (i <- 0 until (header.length)) {
        empStruct(i) = StructField(header(i), StringType, nullable = true)
      }
      StructType(empStruct)
    }

    val quartineEmp = empProcessed.filter(ele => ele(0) != "NA")
    val corruptRecordHeader = spark.sparkContext.parallelize(Array("error_message," + headerProcessed.mkString(",")))
    val finalCorruptRecordRdd = corruptRecordHeader.union(quartineEmp.map(_.mkString(",")))
    finalCorruptRecordRdd.coalesce(1).saveAsTextFile("records/quartine")

    val validEmp = empProcessed.filter(ele => ele(0) == "NA").map(row => Row(row(1), row(2), row(3), row(4), row(5)))

    val schema = createSchema(headerProcessed, testDataType)
    var validEmpDF = spark.sqlContext.createDataFrame(validEmp, schema)

    for (i <- 0 until headerProcessed.length) {
      validEmpDF = validEmpDF.withColumn(headerProcessed(i), validEmpDF(headerProcessed(i)).cast(testDataType(i)))
    }

    val typeMap = validEmpDF.dtypes.map(column =>
      column._2 match {
        case "IntegerType" => (column._1 -> 0)
        case "StringType" => (column._1 -> "")
        case "DoubleType" => (column._1 -> 0.0)
      }).toMap

    val sortedvalidEmpDF = validEmpDF.na.fill(typeMap).coalesce(1).orderBy("name")
    sortedvalidEmpDF.cache()
    sortedvalidEmpDF.show()

    val outputHeader = spark.sparkContext.parallelize(Array(headerProcessed.mkString(",")),1)
    val finalOutputRecordRdd = outputHeader.union(sortedvalidEmpDF.rdd.map(_.mkString(",")))
    finalOutputRecordRdd.coalesce(1).saveAsTextFile("records/output")
  }

}
