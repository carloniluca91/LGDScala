package it.carloni.luca.lgd.spark.common

import org.apache.spark.sql.DataFrame

import scala.collection.immutable.Map

trait SparkStepTrait {

  def run(): Unit

  protected def readCsvFromPathUsingSchema(csvPath: String, pigSchema: Map[String, String]): DataFrame

  protected def writeDataFrameAsCsvToPath(dataFrame: DataFrame, csvPath: String)

}
