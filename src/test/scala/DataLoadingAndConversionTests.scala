import DataTransformation._
import org.scalatest._

class DataLoadingAndConversionTests extends FlatSpec with Matchers {

  val crimeCsv = "/Users/DTAYLOR/Data/crime/2016-04/*/*street.csv"
  val crimeParquet = "/Users/DTAYLOR/Data/crime/parquet/street_crime.parquet"

  val postcodeCsv = "/Users/DTAYLOR/Data/postcode/*.csv"
  val postcodeParquet = "/Users/DTAYLOR/Data/postcode/parquet/postcode.parquet"

  it should "Transform CSV postcode data to Parquet" in {
    val postcode = convertCsvToParquet(postcodeCsv, postcodeParquet)
    postcode.printSchema()
    postcode.show()
    println(postcode.count())
  }

  it should "Transform CSV crime data to Parquet" in {
    val crime = convertCsvToParquet(crimeCsv, crimeParquet)
    crime.printSchema()
    crime.show()
    println(crime.count())
  }
}
