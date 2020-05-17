package it.carloni.luca.lgd.spark.step

import it.carloni.luca.lgd.schema.SofferenzePreviewSchema
import it.carloni.luca.lgd.scopt.config.DtAUfficioConfig
import it.carloni.luca.lgd.spark.common.{AbstractSparkStep, SparkEnums}
import it.carloni.luca.lgd.spark.utils.ScalaUtils.changeDateFormat
import it.carloni.luca.lgd.spark.utils.SparkUtils.changeDateFormatUDF
import org.apache.spark.sql.functions.{col, count, lit, regexp_replace, substring, sum}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.Column
import org.apache.log4j.Logger

class SofferenzePreview extends AbstractSparkStep[DtAUfficioConfig] {

  private val logger = Logger.getLogger(getClass)

  // STEP PATHS
  private val soffOutDirCsvPath = getPropertyValue("sofferenze.preview.soff.outdir.csv")
  private val soffGen2OutputPath = getPropertyValue("sofferenze.preview.soff.gen2")
  private val soffGenSint2OutputPath = getPropertyValue("sofferenze.preview.soff.gen.sint2")

  // STEP SCHEMAS
  private val soffLoadPigSchema = SofferenzePreviewSchema.soffLoadPigSchema

  override def run(dtAUfficioConfig: DtAUfficioConfig): Unit = {

    val dataA: String = dtAUfficioConfig.dataA
    val ufficio: String = dtAUfficioConfig.ufficio

    logger.info(dtAUfficioConfig)

    logger.info(s"sofferenze.preview.soff.outdir.csv: $soffOutDirCsvPath")
    logger.info(s"sofferenze.preview.soff.gen2: $soffGen2OutputPath")
    logger.info(s"sofferenze.preview.soff.gen.sint2: $soffGenSint2OutputPath")

    val dataAOriginalPattern = SparkEnums.DateFormats.DataAFormat.toString
    val dataANewPattern = "yyyy-MM-dd"
    val dataAFormatted = changeDateFormat(dataA, dataAOriginalPattern, dataANewPattern)

    val soffBase = readCsvFromPathUsingSchema(soffOutDirCsvPath, soffLoadPigSchema)
      .select(lit(ufficio).as("ufficio"), lit(dataAFormatted).as("datarif"), col("istituto"), col("ndg"),
        col("numerosofferenza"), col("datainizio"), col("datafine"), col("statopratica"),
        replaceDotWithCommaAndToDouble("saldoposizione"), replaceDotWithCommaAndToDouble("saldoposizionecontab"))

    // 49

    //    , ToString(ToDate(datainizio,'yyyyMMdd'),'yyyy-MM-dd') as datainizio
    //    , ToString(ToDate(datafine,'yyyyMMdd'),'yyyy-MM-dd')   as datafine
    //    , ...
    //    , DoubleConverter(saldoposizione)       as saldoposizione
    //    , DoubleConverter(saldoposizionecontab) as saldoposizionecontab

    val Y4D2M2Format = SparkEnums.DateFormats.Y4M2D2Format.toString
    val soffGen2WindowSpec: WindowSpec = Window.partitionBy("istituto", "ndg", "numerosofferenza")
    val soffGen2 = soffBase
      .withColumn("datainizio", changeDateFormatUDF(col("datainizio"), Y4D2M2Format, dataANewPattern))
      .withColumn("datafine", changeDateFormatUDF(col("datafine"), Y4D2M2Format, dataANewPattern))
      .withColumn("saldoposizione", sum(col("saldoposizione")) over soffGen2WindowSpec)
      .withColumn("saldoposizionecontab", sum(col("saldoposizionecontab")) over soffGen2WindowSpec)

    writeDataFrameAsCsvToPath(soffGen2, soffGen2OutputPath)

    // 85

    // GROUP soff_base BY ( ufficio, datarif, istituto, SUBSTRING(datainizio,0,6), SUBSTRING(datafine,0,6), statopratica );
    // , COUNT(soff_base)        as row_count
    // , SUM(soff_base.saldoposizione)        as saldoposizione
    // , SUM(soff_base.saldoposizionecontab)  as saldoposizionecontab

    val soffGenSint2 = soffBase
      .withColumn("mese_inizio", substring(col("datainizio"), 0, 6))
      .withColumn("mese_fine", substring(col("datafine"), 0, 6))
      .groupBy(col("ufficio"), col("datarif"), col("istituto"),
        col("mese_inizio"), col("mese_fine"), col("statopratica"))
      .agg(count("*").as("row_count"),
        sum(col("saldoposizione")).as("saldoposizione"),
        sum(col("saldoposizionecontab")).as("saldoposizionecontab"))

    // 120

    writeDataFrameAsCsvToPath(soffGenSint2, soffGenSint2OutputPath)
  }

  private def replaceDotWithCommaAndToDouble(columnName: String): Column =

    regexp_replace(col(columnName), ",", ".")
      .cast(DataTypes.DoubleType)
      .as(columnName)
}
