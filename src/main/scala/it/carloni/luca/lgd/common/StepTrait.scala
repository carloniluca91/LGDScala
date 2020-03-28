package it.carloni.luca.lgd.common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

trait StepTrait {

  def run()

  protected def readCsvFromPathUsingSchema(csvPath: String, pigSchema: Map[String, String]): DataFrame

  protected def writeDataFrameAsCsvToPath(dataFrame: DataFrame, csvPath: String)

}
