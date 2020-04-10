package it.carloni.luca.lgd.spark

import scala.collection.immutable.Map
import org.apache.spark.sql.DataFrame

trait SparkStepTrait {

  def run(): Unit

  protected def readCsvFromPathUsingSchema(csvPath: String, pigSchema: Map[String, String]): DataFrame

  protected def writeDataFrameAsCsvToPath(dataFrame: DataFrame, csvPath: String)

}
