import org.apache.spark.sql._
import Spark._

object DataTransformation {
  private def renameFields(df : DataFrame, fields : List[String]) : DataFrame = fields match {
    case Nil => df
    case field :: tail =>
      val safeName = field.toLowerCase().replaceAll("[^a-z0-9]+", "_")
      renameFields(df.withColumnRenamed(field, safeName), tail)
  }

  def convertCsvToParquet(inputData: String, outputData: String): DataFrame = {
    val csvData = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(inputData)
      .toDF()

    val data = renameFields(csvData, csvData.schema.fieldNames.toList)

    scalax.file.Path.fromString(outputData).deleteRecursively()
    data.write.format("parquet").save(outputData)

    data
  }
}
