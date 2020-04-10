package it.carloni.luca.lgd.spark

import scala.collection.immutable.Map

import it.carloni.luca.lgd.commons.LGDCommons
import it.carloni.luca.lgd.spark.udfs.{SparkUDFs, UDFsNames}
import org.apache.commons.configuration.{ConfigurationException, PropertiesConfiguration}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.log4j.Logger

abstract class AbstractSparkStep extends SparkStepTrait {

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

  private def getSparkSessionWithUDFs: SparkSession = {

    val sparkSession = SparkSession.builder().getOrCreate()

    // UDF REGISTRATION
    sparkSession.udf.register(UDFsNames.AddDurationUDFName, SparkUDFs.addDurationUDF)
    sparkSession.udf.register(UDFsNames.ChangeDateFormatUDFName, SparkUDFs.changeDateFormatUDF)
    sparkSession.udf.register(UDFsNames.DaysBetweenUDFName, SparkUDFs.daysBetweenUDF)
    sparkSession.udf.register(UDFsNames.LeastDateUDFName, SparkUDFs.leastDateUDF)
    sparkSession.udf.register(UDFsNames.SubtractDurationUDFName, SparkUDFs.subtractDurationUDF)
    sparkSession
  }


  private def fromPigSchemaToStructType(columnMap: Map[String, String]): StructType = {

    val structFieldSeq: Seq[StructField] = (for ((key, value) <- columnMap) yield {

      val columnName: String = key
      val columnType: DataType = value match {

        case "chararray" => DataTypes.StringType
        case "int" => DataTypes.IntegerType
        case "double" => DataTypes.DoubleType
        case _ => throw new Exception(s"Unsupported data type: $columnType")
      }

      StructField(columnName, columnType)
    }).toSeq

    StructType(structFieldSeq)
  }

  private def loadProperties(): Unit = {

    try {

      lgdProperties.load(getClass
        .getClassLoader
        .getResourceAsStream("lgd.properties"))
    }
    catch {

      case exception: ConfigurationException =>

        logger.error("ConfigurationException occurred")
        logger.error(exception)
        throw exception
    }
  }

  protected def getPropertyValue(propertyName: String): String = lgdProperties.getString(propertyName)

  protected def readCsvFromPathUsingSchema(csvPath: String, pigSchema: Map[String, String]): DataFrame = {

    val sparkStructType: StructType = fromPigSchemaToStructType(pigSchema)
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
