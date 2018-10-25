import Spark._
import org.apache.log4j.{Level, Logger}
import org.scalatest._
import org.apache.spark.sql.functions._
import Udfs._

class JoinHousePriceToCrime extends FlatSpec with Matchers with BeforeAndAfter {

  val crimeParquet = "/Users/DTAYLOR/Data/crime/parquet/street_crime.parquet"
  val postcodeParquet = "/Users/DTAYLOR/Data/postcode/parquet"
  val housePriceParquet = "/Users/DTAYLOR/Data/house_price/parquet"

  it should "count crimes" in {
    spark.sql("select count(*) from crime").collect().foreach(println)
  }

  it should "show max date on house price data" in {
    spark.sql("select max(date), count(*) from house_prices").collect().foreach(println)
  }

  it should "most expensive town from house price data" in {
    spark.sql("select town, avg(price) as price from house_prices group by town order by price desc").collect().foreach(println)
  }

  it should "show crimes by postcode" in {
    // If postcodes don't work - try GeoHashes!
    val geoHashPrecision = 7

    spark
      .sql("select * from crime")
      .withColumn("geohash", withGeoHash(geoHashPrecision)(col("latitude"), col("longitude")))
      .groupBy("geohash")
      .agg(count("*") as "crime_count")
      .createOrReplaceTempView("crime_by_geohash")

    spark.sql("select * from postcodes")
      .withColumn("geohash", withGeoHash(geoHashPrecision)(col("latitude"), col("longitude")))
      .createOrReplaceTempView("postcode_with_geohash")

    val crimeByGeoHash = spark.sql("select * from crime_by_geohash c left outer join postcode_with_geohash p on (c.geohash == p.geohash)")

    crimeByGeoHash.printSchema()
    crimeByGeoHash.show()
  }

  val crime_types = Seq("Possession of weapons", "Theft from the person", "Criminal damage and arson", "Other theft",
    "Shoplifting", "Anti-social behaviour", "Public disorder and weapons", "Violence and sexual offences",
    "Burglary", "Vehicle crime", "Drugs", "Bicycle theft", "Violent crime", "Robbery", "Other crime", "Public order")

  it should "fetch the distinct crime types" in {
    spark.sql("select distinct(crime_type) from crime")
      .collect()
      .foreach(println)
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
