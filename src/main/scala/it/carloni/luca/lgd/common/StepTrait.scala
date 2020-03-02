package it.carloni.luca.lgd.common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

trait StepTrait {

  abstract def run()

  abstract protected def readCsvFromPathUsingSchema(csvPath: String, schema: StructType): DataFrame

  abstract protected def writeDataFrameAsCsvToPath(dataFrame: DataFrame, csvPath: String)

}
