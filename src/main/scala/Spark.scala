import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Spark {
  val session = SparkSession
    .builder()
    .appName("Society")
    .master("local[8]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.default.parallelism", "8")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()

  Logger.getRootLogger.setLevel(Level.ERROR)
}
