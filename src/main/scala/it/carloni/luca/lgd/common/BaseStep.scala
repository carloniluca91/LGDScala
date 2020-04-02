package it.carloni.luca.lgd.common

import it.carloni.luca.lgd.common.utils.{ScalaUtils, LGDCommons}
import it.carloni.luca.lgd.common.udfs.{SparkUDFs, UDFsNames}
import org.apache.commons.configuration.{ConfigurationException, PropertiesConfiguration}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.log4j.Logger

abstract class BaseStep extends StepTrait {

  private final val logger: Logger = Logger.getLogger(getClass)
  private final val sparkSession: SparkSession = getSparkSessionWithUDFs
  private final val lgdProperties: PropertiesConfiguration = new PropertiesConfiguration
  loadProperties()

  private final val csvFormat: String = LGDCommons.CSV.SparkCsvFormat
  private final val csvInputDelimiter: String = LGDCommons.CSV.InputDelimiter
  private final val csvOutputDelimiter: String = LGDCommons.CSV.OutputDelimiter

  logger.debug(s"csvFormat : $csvFormat")
  logger.debug(s"csvInputDelimiter: $csvInputDelimiter")
  logger.debug(s"csvOutputDelimiter:  $csvOutputDelimiter")

  private def getSparkSessionWithUDFs: SparkSession = registerUDFS(SparkSession.builder().getOrCreate())

  private def registerUDFS(sparkSession: SparkSession): SparkSession = {

    sparkSession.udf.register(UDFsNames.AddDurationUDFName, SparkUDFs.addDurationUDF)
    sparkSession.udf.register(UDFsNames.ChangeDateFormatUDFName, SparkUDFs.changeDateFormatUDF)
    sparkSession.udf.register(UDFsNames.DaysBetweenUDFName, SparkUDFs.daysBetweenUDF)
    sparkSession.udf.register(UDFsNames.LeastDateUDFName, SparkUDFs.leastDateUDF)
    sparkSession.udf.register(UDFsNames.SubtractDurationUDFName, SparkUDFs.subtractDurationUDF)
    sparkSession
  }

  private def fromPigSchemaToStructType(columnMap: Map[String, String]) = new StructType(columnMap.map(ScalaUtils.getTypedStructField).toArray)

  private def loadProperties(): Unit = {

    // TRY TO LOAD PROPERTIES
    try lgdProperties.load(getClass.getClassLoader.getResourceAsStream("lgd.properties"))
    catch {
      case exception: ConfigurationException =>
        logger.error("ConfigurationException occurred")
        logger.error(s"exception.getMessage: ${exception.getMessage}")
        logger.error(exception)
    }
  }

  protected def getPropertyValue(propertyName: String): String = lgdProperties.getString(propertyName)

  protected def readCsvFromPathUsingSchema(csvPath: String, pigSchema: Map[String, String]): DataFrame = {

    val sparkStructType = fromPigSchemaToStructType(pigSchema)
    sparkSession.read.
      format(csvFormat)
      .option("sep", csvInputDelimiter)
      .schema(sparkStructType)
      .csv(csvPath)
  }

  protected def writeDataFrameAsCsvToPath(dataFrame: DataFrame, csvPath: String): Unit = {

    dataFrame.coalesce(1)
      .write
      .format(csvFormat)
      .option("sep", csvOutputDelimiter)
      .mode(SaveMode.Overwrite)
      .csv(csvPath)
  }
}
