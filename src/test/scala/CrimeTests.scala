import org.apache.spark.sql.DataFrame
import org.scalatest._

class CrimeTests extends FlatSpec with Matchers {

  val allCsvData = "/Users/DTAYLOR/Data/crime/2016-04/*/*street.csv"
  val allParquetData = "/Users/DTAYLOR/Data/crime/parquet/street_crime.parquet"

  def renameFields(df : DataFrame, fields : List[String]) : DataFrame = fields match {
    case Nil => df
    case field :: tail =>
      val safeName = field.toLowerCase().replaceAll("[^a-z0-9]+", "_")
      renameFields(df.withColumnRenamed(field, safeName), tail)
  }

  it should "load csv data then save it to parquet with extra fields" in {
    val csvData = Spark
      .session
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(allCsvData)
      .toDF()

    csvData.printSchema()
    val data = renameFields(csvData, csvData.schema.fieldNames.toList)
    data.printSchema()
    data.show()
    println(data.count())

    scalax.file.Path.fromString(allParquetData).deleteRecursively()

    data.write.format("parquet").save(allParquetData)
  }
}
