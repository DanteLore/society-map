import org.apache.spark.sql.functions._

object Udfs {
  val withDate = udf((s: String) => {
    s.substring(0, 7)
  })

  def withGeoHash(precision : Int) = udf((lat: Double, lon: Double) => {
    GeoHash.encode(lat, lon, precision)
  })
}
