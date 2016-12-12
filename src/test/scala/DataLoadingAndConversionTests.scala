import DataTransformation._
import org.apache.log4j.{Level, Logger}
import org.scalatest._
import org.apache.spark.sql.functions._
import Udfs._

class DataLoadingAndConversionTests extends FlatSpec with Matchers with BeforeAndAfter {

  val crimeCsv = "/Users/DTAYLOR/Data/crime/2016-04/*/*street.csv"
  val crimeParquet = "/Users/DTAYLOR/Data/crime/parquet/street_crime.parquet"

  val postcodeCsv = "/Users/DTAYLOR/Data/postcode/*.csv"
  val postcodeParquet = "/Users/DTAYLOR/Data/postcode/parquet"

  val housePriceCsv = "/Users/DTAYLOR/Data/house_price/*.csv"
  val housePriceParquet = "/Users/DTAYLOR/Data/house_price/parquet"

  // http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv
  it should "Transform CSV house price data to Parquet" in {
    val housePrices =
      readCsvData(housePriceCsv)
        .withColumn("month", withDate(col("date")))
    housePrices.printSchema()
    housePrices.show()
    writeParquet(housePriceParquet, housePrices)
  }

  it should "Transform CSV postcode data to Parquet" in {
    val postcode = readCsvData(postcodeCsv)
    postcode.printSchema()
    postcode.show()
    writeParquet(postcodeParquet, postcode)
  }

  // https://data.police.uk/data/archive/
  it should "Transform CSV crime data to Parquet" in {
    val crime = readCsvData(crimeCsv)
    crime.printSchema()
    crime.show()
    writeParquet(crimeParquet, crime)
  }

  before {
    Logger.getRootLogger.setLevel(Level.ERROR)
  }
}
