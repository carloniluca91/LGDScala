package it.carloni.luca.lgd.step

import it.carloni.luca.lgd.commons.LGDCommons
import it.carloni.luca.lgd.spark.AbstractSparkStep
import it.carloni.luca.lgd.schema.QuadFcollSchema
import it.carloni.luca.lgd.spark.utils.SparkUtils.changeDateFormat
import org.apache.spark.sql.functions.{coalesce, col}
import org.apache.log4j.Logger

class QuadFcoll extends AbstractSparkStep {

  private val logger = Logger.getLogger(getClass)

  // STEP PATHS
  private val fcollCsvPath = getPropertyValue("quad.fcoll.fcoll.csv")
  private val oldFposiCsvPath = getPropertyValue("quad.fcoll.oldfposi.csv")
  private val fcollOutDistOutputPath = getPropertyValue("quad.fcoll.fileoutdist")

  // STEP SCHEMAS
  private val fcollLoadPigSchema = QuadFcollSchema.fcollLoadPigSchema
  private val oldFposiLoadPigSchema = QuadFcollSchema.oldFposiLoadPigSchema

  override def run(): Unit = {

    logger.debug(s"fcollCsvPath: $fcollCsvPath")
    logger.debug(s"oldFposiCsvPath: $oldFposiCsvPath")
    logger.debug(s"fcollOutDistOutputPath: $fcollOutDistOutputPath")

    val D2M3Y4Format = "ddMMMyyyy"
    val Y4M2D2Format = LGDCommons.DatePatterns.Y4M2D2Pattern

    // ,ToString(ToDate( data_inizio_DEF,'ddMMMyyyy'),'yyyyMMdd')    as data_inizio_DEF
    // ,ToString(ToDate( data_collegamento,'ddMMMyyyy'),'yyyyMMdd')  as data_collegamento

    val fcoll = readCsvFromPathUsingSchema(fcollCsvPath, fcollLoadPigSchema)
      .withColumn("data_inizio_DEF", changeDateFormat(col("data_inizio_DEF"), D2M3Y4Format, Y4M2D2Format))
      .withColumn("data_collegamento", changeDateFormat(col("data_collegamento"), D2M3Y4Format, Y4M2D2Format))

    // 39

    // FILTER oldfposi_load BY dataINIZIOPD is not null OR datainizioinc is not null OR dataSOFFERENZA is not null
    // ToString(ToDate( datainizioDEF,'yy-MM-dd'),'yyyyMMdd')   as datainizioDEF
    // ,ToString(ToDate( dataFINEDEF,'yy-MM-dd'),'yyyyMMdd')   as dataFINEDEF

    val Y2_M2_D2Format = "yy-MM-dd"
    val oldFposi = readCsvFromPathUsingSchema(oldFposiCsvPath, oldFposiLoadPigSchema)
      .filter(col("dataINIZIOPD").isNotNull ||  col("datainizioinc").isNotNull || col("dataSOFFERENZA").isNotNull)
      .withColumn("datainizioDEF", changeDateFormat(col("datainizioDEF"), Y2_M2_D2Format, Y4M2D2Format))
      .withColumn("dataFINEDEF", changeDateFormat(col("dataFINEDEF"), Y2_M2_D2Format, Y4M2D2Format))

    // 76

    // JOIN oldfposi BY (cumulo) LEFT, fcoll BY (cumulo);
    // ,( fcoll::data_inizio_DEF   is null ? oldfposi::datainizioDEF : fcoll::data_inizio_DEF )     as DATA_DEFAULT
    // ,( fcoll::cd_istituto_COLL  is null ? oldfposi::codicebanca   : fcoll::cd_istituto_COLL )    as ISTITUTO_COLLEGATO
    // ,( fcoll::ndg_COLL          is null ? oldfposi::ndgprincipale : fcoll::ndg_COLL )            as NDG_COLLEGATO
    // ,( fcoll::data_collegamento is null ? oldfposi::datainizioDEF : fcoll::data_collegamento )   as DATA_COLLEGAMENTO

    val fileOutDist = oldFposi.join(fcoll, Seq("cumulo"), "left")
      .select(oldFposi("codicebanca"), oldFposi("ndgprincipale"), oldFposi("datainizioDEF"), oldFposi("dataFINEDEF"),
        coalesce(fcoll("data_inizio_DEF"), oldFposi("datainizioDEF")).as("DATA_DEFAULT"),
        coalesce(fcoll("cd_istituto_COLL"), oldFposi("codicebanca")).as("ISTITUTO_COLLEGATO"),
        coalesce(fcoll("ndg_COLL"), oldFposi("ndgprincipale")).as("NDG_COLLEGATO"),
        coalesce(fcoll("data_collegamento"), oldFposi("datainizioDEF")).as("DATA_COLLEGAMENTO"),
        fcoll("cumulo"))
      .distinct()

    writeDataFrameAsCsvToPath(fileOutDist, fcollOutDistOutputPath)
  }
}
