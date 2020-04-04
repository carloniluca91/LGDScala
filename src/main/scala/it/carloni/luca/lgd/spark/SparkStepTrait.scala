package it.carloni.luca.lgd.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

trait SparkStepTrait {

  def run(): Unit

  protected def readCsvFromPathUsingSchema(csvPath: String, pigSchema: Map[String, String]): DataFrame

  protected def writeDataFrameAsCsvToPath(dataFrame: DataFrame, csvPath: String)

}
