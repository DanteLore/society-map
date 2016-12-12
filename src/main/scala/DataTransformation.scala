import org.apache.spark.sql._
import Spark._

object DataTransformation {
  private def renameFields(df : DataFrame, fields : List[String]) : DataFrame = fields match {
    case Nil => df
    case field :: tail =>
      val safeName = field.toLowerCase().replaceAll("[^a-z0-9]+", "_")
      renameFields(df.withColumnRenamed(field, safeName), tail)
  }

  def readCsvData(inputData: String) : DataFrame = {
    val csvData = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(inputData)
      .toDF()

    renameFields(csvData, csvData.schema.fieldNames.toList)
  }

  def writeParquet(outputFilename : String, data : DataFrame):Unit = {
    scalax.file.Path.fromString(outputFilename).deleteRecursively()
    data.write.format("parquet").save(outputFilename)
  }
}
