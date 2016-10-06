import org.apache.spark.sql.functions._
import org.scalatest._

class CrimeTests extends FlatSpec with Matchers {

  val allCsvData = "/Users/DTAYLOR/Data/crime/2016-04/*/*street.csv"
  val allParquetData = "/Users/DTAYLOR/Data/crime/parquet/street_crime.parquet"

  it should "load csv data then save it to parquet with extra fields" in {

    import Spark.sqlContext.implicits._

    val crimes = Spark.sc
      .textFile(allCsvData, 1)
      .map(_.split(','))
      .toDF()

    crimes.printSchema()
    crimes.show()
    println(crimes.count())

    scalax.file.Path.fromString(allParquetData).deleteRecursively()

    //filteredAndEnriched.write.format("parquet").save(parquetData2011)
  }
}
