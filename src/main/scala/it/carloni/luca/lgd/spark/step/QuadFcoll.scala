package it.carloni.luca.lgd.spark.step

import it.carloni.luca.lgd.schema.QuadFcollSchema
import it.carloni.luca.lgd.scopt.config.EmptyConfig
import it.carloni.luca.lgd.spark.common.{AbstractSparkStep, SparkEnums}
import it.carloni.luca.lgd.spark.utils.SparkUtils.changeDateFormatUDF
import org.apache.spark.sql.functions.{callUDF, coalesce, col, lit}
import org.apache.log4j.Logger
import org.apache.spark.sql.Column

import scala.collection.mutable

class QuadFcoll extends AbstractSparkStep[EmptyConfig] {

  private val logger = Logger.getLogger(getClass)

  // STEP PATHS
  private val fcollCsvPath = getPropertyValue("quad.fcoll.fcoll.csv")
  private val oldFposiCsvPath = getPropertyValue("quad.fcoll.oldfposi.csv")
  private val fcollOutDistOutputPath = getPropertyValue("quad.fcoll.fileoutdist")

  // STEP SCHEMAS
  private val fcollLoadPigSchema: mutable.LinkedHashMap[String, String] = QuadFcollSchema.fcollLoadPigSchema
  private val oldFposiLoadPigSchema: mutable.LinkedHashMap[String, String] = QuadFcollSchema.oldFposiLoadPigSchema

  override def run(emptyConfig: EmptyConfig): Unit = {

    logger.info(s"quad.fcoll.fcoll.csv: $fcollCsvPath")
    logger.info(s"quad.fcoll.oldfposi.csv: $oldFposiCsvPath")
    logger.info(s"quad.fcoll.fileoutdist: $fcollOutDistOutputPath")

    val D2M2Y4Format = "ddMMyyyy"
    val Y4M2D2Format = SparkEnums.DateFormats.Y4M2D2Format.toString

    // ,ToString(ToDate( data_inizio_DEF,'ddMMyyyy'),'yyyyMMdd')    as data_inizio_DEF
    // ,ToString(ToDate( data_collegamento,'ddMMyyyy'),'yyyyMMdd')  as data_collegamento

    val fcoll = readCsvFromPathUsingSchema(fcollCsvPath, fcollLoadPigSchema)
      .withColumn("data_inizio_DEF", changeDateFormatUDF(col("data_inizio_DEF"), D2M2Y4Format, Y4M2D2Format))
      .withColumn("data_collegamento", changeDateFormatUDF(col("data_collegamento"), D2M2Y4Format, Y4M2D2Format))

    // 39

    // FILTER oldfposi_load BY dataINIZIOPD is not null OR datainizioinc is not null OR dataSOFFERENZA is not null
    // ToString(ToDate( datainizioDEF,'yy-MM-dd'),'yyyyMMdd')   as datainizioDEF
    // ,ToString(ToDate( dataFINEDEF,'yy-MM-dd'),'yyyyMMdd')   as dataFINEDEF

    val Y2_M2_D2Format = "yy-MM-dd"
    val oldFposi = readCsvFromPathUsingSchema(oldFposiCsvPath, oldFposiLoadPigSchema)
      .filter(col("dataINIZIOPD").isNotNull ||  col("datainizioinc").isNotNull || col("dataSOFFERENZA").isNotNull)
      .withColumn("datainizioDEF", changeDateFormatFromY2toY4(col("datainizioDEF"), Y2_M2_D2Format, Y4M2D2Format))
      .withColumn("dataFINEDEF", changeDateFormatFromY2toY4(col("dataFINEDEF"), Y2_M2_D2Format, Y4M2D2Format))

    // 76

    // JOIN oldfposi BY (cumulo) LEFT, fcoll BY (cumulo);
    // ,( fcoll::data_inizio_DEF   is null ? oldfposi::datainizioDEF : fcoll::data_inizio_DEF )     as DATA_DEFAULT
    // ,( fcoll::cd_istituto_COLL  is null ? oldfposi::codicebanca   : fcoll::cd_istituto_COLL )    as ISTITUTO_COLLEGATO
    // ,( fcoll::ndg_COLL          is null ? oldfposi::ndgprincipale : fcoll::ndg_COLL )            as NDG_COLLEGATO
    // ,( fcoll::data_collegamento is null ? oldfposi::datainizioDEF : fcoll::data_collegamento )   as DATA_COLLEGAMENTO

    val fileOutDist = oldFposi.join(fcoll, oldFposi("cumulo") === fcoll("cumulo"), "left")
      .select(oldFposi("codicebanca"), oldFposi("ndgprincipale"), oldFposi("datainizioDEF"), oldFposi("dataFINEDEF"),
        coalesce(fcoll("data_inizio_DEF"), oldFposi("datainizioDEF")).as("DATA_DEFAULT"),
        coalesce(fcoll("cd_istituto_COLL"), oldFposi("codicebanca")).as("ISTITUTO_COLLEGATO"),
        coalesce(fcoll("ndg_COLL"), oldFposi("ndgprincipale")).as("NDG_COLLEGATO"),
        coalesce(fcoll("data_collegamento"), oldFposi("datainizioDEF")).as("DATA_COLLEGAMENTO"),
        fcoll("cumulo"))
      .distinct()

    writeDataFrameAsCsvToPath(fileOutDist, fcollOutDistOutputPath)
  }

  private def changeDateFormatFromY2toY4(column: Column, oldFormat: String, newFormat: String): Column =

    callUDF(SparkEnums.UDFsNames.ChangeDateFormatFromY2toY4UDFName.toString,
      column,
      lit(oldFormat),
      lit(newFormat))
}
