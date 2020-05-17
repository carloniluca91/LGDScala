package it.carloni.luca.lgd.spark.common

import it.carloni.luca.lgd.scopt.config.StepConfig
import it.carloni.luca.lgd.spark.common.SparkEnums.{CSV, UDFsNames}
import it.carloni.luca.lgd.spark.udfs.SparkUDFs
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

abstract class AbstractSparkStep[T <: StepConfig] extends SparkStepTrait[T] {

  private final val logger: Logger = Logger.getLogger(getClass)
  private final val sparkSession: SparkSession = getSparkSessionWithUDFs
  private final val lgdProperties: PropertiesConfiguration = new PropertiesConfiguration
  loadProperties()

  private final val csvFormat: String = CSV.SparkCsvFormat.toString
  private final val csvInputDelimiter: String = CSV.InputDelimiter.toString
  private final val csvOutputDelimiter: String =CSV.OutputDelimiter.toString

  logger.info(s"Spark csv format : $csvFormat")
  logger.info(s"csv input delimiter: $csvInputDelimiter")
  logger.info(s"csv output delimiter:  $csvOutputDelimiter")

  private def getSparkSessionWithUDFs: SparkSession = {

    val sparkSession = SparkSession.builder().getOrCreate()

    logger.info("Successfully initialized SparkSession")
    logger.info(s"Spark application UI available @ : ${sparkSession.sparkContext.uiWebUrl.get}")

    // UDF REGISTRATION
    sparkSession.udf.register(UDFsNames.AddDurationUDFName.toString, SparkUDFs.addDurationUDF)
    sparkSession.udf.register(UDFsNames.ChangeDateFormatUDFName.toString, SparkUDFs.changeDateFormatUDF)
    sparkSession.udf.register(UDFsNames.ChangeDateFormatFromY2toY4UDFName.toString, SparkUDFs.changeDateFormatFromY2toY4UDF)
    sparkSession.udf.register(UDFsNames.DaysBetweenUDFName.toString, SparkUDFs.daysBetweenUDF)
    sparkSession.udf.register(UDFsNames.LeastDateUDFName.toString, SparkUDFs.leastDateUDF)
    sparkSession.udf.register(UDFsNames.SubtractDurationUDFName.toString, SparkUDFs.subtractDurationUDF)

    logger.info("Successfully registered user-defined functions (UDFs)")

    sparkSession
  }

  private def fromPigSchemaToStructType(columnMap: mutable.LinkedHashMap[String, String]): StructType = {

    val structFieldSeq: Seq[StructField] = (for ((key, value) <- columnMap) yield {

      val columnName: String = key
      val columnType: DataType = value match {

        case "chararray" => DataTypes.StringType
        case "int" => DataTypes.IntegerType
        case "double" => DataTypes.DoubleType
        case _ => throw new Exception(s"Unsupported data type: $value")
      }

      StructField(columnName, columnType)
    }).toSeq

    StructType(structFieldSeq)
  }

  private def loadProperties(): Unit = {

    val loadPropertiesTry: Try[Unit] = Try(lgdProperties.load(getClass.getClassLoader.getResourceAsStream("lgd.properties")))
    loadPropertiesTry match {

      case Failure(exception) =>

        logger.error("Exception occurred while loading properties file")
        logger.error(exception)
        throw exception

      case Success(_) => logger.info("Successfully loaded properties file")
    }
  }

  protected def getPropertyValue(propertyName: String): String = lgdProperties.getString(propertyName)

  protected def readCsvFromPathUsingSchema(csvPath: String, pigSchema: mutable.LinkedHashMap[String, String]): DataFrame = {

    logger.info(s"Starting to load data from file $csvPath")

    val sparkStructType: StructType = fromPigSchemaToStructType(pigSchema)
    val csvDataframe: DataFrame = sparkSession.read.
      format(csvFormat)
      .option("sep", csvInputDelimiter)
      .schema(sparkStructType)
      .csv(csvPath)

    logger.info(s"Successfully loaded data from file $csvPath")
    csvDataframe
  }

  protected def writeDataFrameAsCsvToPath(dataFrame: DataFrame, csvPath: String): Unit = {

    logger.info(s"Starting to save data at path $csvPath")

    dataFrame.coalesce(1)
      .write
      .format(csvFormat)
      .option("sep", csvOutputDelimiter)
      .option("header", value = true)
      .mode(SaveMode.Overwrite)
      .csv(csvPath)

    logger.info(s"Successfully saved data at path $csvPath")
  }
}
