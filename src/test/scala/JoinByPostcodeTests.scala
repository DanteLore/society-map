import Spark._
import org.apache.log4j.{Level, Logger}
import org.scalatest._

class JoinByPostcodeTests extends FlatSpec with Matchers with BeforeAndAfter {

  val crimeParquet = "/Users/DTAYLOR/Data/crime/parquet/street_crime.parquet"
  val postcodeParquet = "/Users/DTAYLOR/Data/postcode/parquet"
  val housePriceParquet = "/Users/DTAYLOR/Data/house_price/parquet"

  it should "show crimes by postcode" in {
    val crimeLocations = spark.sql("select distinct(latitude, longitude) from crime")
    spark.sql("select * from crime").show()
    spark.sql("select month, crime_type, latitude, longitude, count(*) from crime group by month, latitude, longitude, crime_type").show()

    // WORK IN PROGRESS!

    // Joining here is hard - because there is no relationship between the lat-long of a crime and the lat-long of a postcode :(
  }

  it should "Count the number of distinct crime locations and postcodes" in {
    val crimeLocationCount = spark.sql("select distinct(latitude, longitude) from crime").count()
    val postcodeCount = spark.sql("select distinct(postcode) from postcodes").count()

    println(s"$crimeLocationCount crime locations") //  834786
    println(s"$postcodeCount postcodes ")           // 1812402
  }

  it should "See if any records from the crime data join on postcode lat-long" in {
    val joined = spark.sql("select * from crime c left outer join postcodes p on (c.latitude == p.latitude and c.longitude == p.longitude)")
    joined.show()
    println(s"Number of rows with a match ${joined.count()}") // 0 :-(
  }

  it should "Join postcode and house price data based on postcode" in {
    val postcodes = spark.sql("select * from postcodes p join house_prices h on (p.postcode == h.postcode)")

    postcodes.printSchema()
    postcodes.show()
  }

  before {
    Logger.getRootLogger.setLevel(Level.ERROR)

    spark
      .read
      .parquet(postcodeParquet)
      .createOrReplaceTempView("postcodes")

    spark
      .read
      .parquet(crimeParquet)
      .createOrReplaceTempView("crime")

    spark
      .read
      .parquet(housePriceParquet)
      .createOrReplaceTempView("house_prices")
  }
}
