package it.carloni.luca.lgd.common

import it.carloni.luca.lgd.common.utils.{JavaUtils, LGDCommons}
import it.carloni.luca.lgd.common.udfs.{SparkUDFs, UDFsNames}
import org.apache.commons.configuration.{ConfigurationException, PropertiesConfiguration}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.log4j.Logger

abstract class AbstractStep extends StepTrait {

  private val logger: Logger = Logger.getLogger(getClass)
  private val lgdProperties: PropertiesConfiguration = new PropertiesConfiguration
  private val sparkSession: SparkSession = getSparkSessionWithUDFs

  // TRY TO LOAD PROPERTIES
  try lgdProperties.load(getClass.getClassLoader.getResourceAsStream("./lgd.properties"))
  catch {
    case exception: ConfigurationException =>
      logger.error("ConfigurationException occurred")
      logger.error(f"exception.getMessage: ${exception.getMessage}")
      logger.error(exception)
  }

  private val csvFormat: String = LGDCommons.CSV.SparkFormat
  private val csvInputDelimiter: String = LGDCommons.CSV.InputDelimiter
  private val csvOutputDelimiter: String = LGDCommons.CSV.OutputDelimiter

  logger.debug(f"csvFormat : $csvFormat")
  logger.debug(f"csvInputDelimiter: $csvInputDelimiter")
  logger.debug(f"csvOutputDelimiter:  $csvOutputDelimiter")

  protected def getValue(propertyName: String): String = lgdProperties.getString(propertyName)

  private def getSparkSessionWithUDFs: SparkSession = registerUDFS(SparkSession.builder().getOrCreate())

  private def registerUDFS(sparkSession: SparkSession): SparkSession = {

    sparkSession.udf.register(UDFsNames.AddDurationUDFName, SparkUDFs.addDurationUDF)
    sparkSession.udf.register(UDFsNames.SubtractDurationUDFName, SparkUDFs.subtractDurationUDF)
    sparkSession.udf.register(UDFsNames.IsDateGeqOtherDateUDFName, SparkUDFs.isDateGeqOtherDateUDF)
    sparkSession.udf.register(UDFsNames.IsDateLeqOtherDateUDFName, SparkUDFs.isDateLeqOtherDateUDF)
    sparkSession.udf.register(UDFsNames.LeastDateUDFName, SparkUDFs.leastDateUDF)
    sparkSession
  }

  protected def fromPigSchemaToStructType(columnMap: Map[String, String]) = new StructType(columnMap.map(JavaUtils.getTypedStructField).toArray)

  protected def readCsvFromPathUsingSchema(csvPath: String, schema: StructType): DataFrame =
    sparkSession.read.format(csvFormat).option("sep", csvInputDelimiter).schema(schema).csv(csvPath)

  protected def writeDataFrameAsCsvToPath(dataFrame: DataFrame, csvPath: String): Unit =
    dataFrame.write.format(csvFormat).option("sep", csvOutputDelimiter).mode(SaveMode.Overwrite).csv(csvPath)

}
