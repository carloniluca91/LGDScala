package it.carloni.luca.lgd.spark.step

import it.carloni.luca.lgd.schema.FrappNdgMonthlySchema
import it.carloni.luca.lgd.scopt.config.DtANumeroMesi12Config
import it.carloni.luca.lgd.spark.common.{AbstractSparkStep, SparkEnums}
import it.carloni.luca.lgd.spark.utils.SparkUtils.{addDurationUDF, leastDateUDF, subtractDurationUDF, toIntType, toStringType}
import it.carloni.luca.lgd.spark.utils.ScalaUtils.changeDateFormat
import org.apache.spark.sql.functions.{col, substring}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.log4j.Logger

import scala.collection.mutable

class FrappNdgMonthly extends AbstractSparkStep[DtANumeroMesi12Config] {

  private val logger = Logger.getLogger(getClass)

  // STEP PATHS
  private val cicliNdgCsvPath = getPropertyValue("frapp.ndg.monthly.cicli.ndg.path.csv")
  private val tlburttCsvPath = getPropertyValue("frapp.ndg.monthly.tlburtt.csv")
  private val tlbcidefTlburttOutputPath = getPropertyValue("frapp.ndg.monthly.tlbcidef.tlburtt")

  // STEP SCHEMAS
  private val tlbcidefPigSchema: mutable.LinkedHashMap[String, String] = FrappNdgMonthlySchema.tlbcidefPigSchema
  private val tlburttPigSchema: mutable.LinkedHashMap[String, String] = FrappNdgMonthlySchema.tlburttPigSchema

  override def run(dtANumeroMesi12Config: DtANumeroMesi12Config): Unit = {

    val dataA: String = dtANumeroMesi12Config.dataA
    val numeroMesi1: Int = dtANumeroMesi12Config.numeroMesi1
    val numeroMesi2: Int = dtANumeroMesi12Config.numeroMesi2

    logger.info(dtANumeroMesi12Config.toString)

    logger.info(s"frapp.ndg.monthly.cicli.ndg.path.csv: $cicliNdgCsvPath")
    logger.info(s"frapp.ndg.monthly.tlburtt.csv: $tlburttCsvPath")
    logger.info(s"frapp.ndg.monthly.tlbcidef.tlburtt: $tlbcidefTlburttOutputPath")

    val tlbcidef = readCsvFromPathUsingSchema(cicliNdgCsvPath, tlbcidefPigSchema)
    val cicliNdgPrinc = tlbcidef.filter(col("cd_collegamento").isNull)
    val cicliNdgColl = tlbcidef.filter(col("cd_collegamento").isNotNull)

    // 60

    val tlburttFilter = readCsvFromPathUsingSchema(tlburttCsvPath, tlburttPigSchema)
      .filter(col("progr_segmento") === 0)

    // 111

    val Y4M2D2Format = "yyyyMMdd"
    val dataAFormat: String = SparkEnums.DateFormats.DataAFormat.toString
    val dataAFormatted = changeDateFormat(dataA, dataAFormat, Y4M2D2Format)
    val tlbcidefTlburtt: DataFrame = Seq(cicliNdgPrinc, cicliNdgColl)
      .map((cicliNdgDf: DataFrame) => {

      // JOIN cicli_ndg_princ BY (codicebanca_collegato, ndg_collegato), tlburtt_filter BY (cd_istituto, ndg);
      // OIN  cicli_ndg_coll BY (codicebanca_collegato, ndg_collegato), tlburtt_filter BY (cd_istituto, ndg);

      val joinConditionCol = (cicliNdgDf("codicebanca_collegato") === tlburttFilter("cd_istituto")) &&
        (cicliNdgDf("ndg_collegato") === tlburttFilter("ndg"))

      // FILTER BY ToDate((chararray)dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')

      val dtRIferimentoDataInizioDefFilterCol = tlburttFilter("dt_riferimento") >=
        toIntType(subtractDurationUDF(cicliNdgDf("datainiziodef"), Y4M2D2Format, numeroMesi1))

      // SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M')
      // LeastDate( (int)ToString(SubtractDuration(....)
      // AddDuration( ToDate( (chararray) LeastDate(....)

      val substractDurationCol = subtractDurationUDF(cicliNdgDf("datafinedef"), Y4M2D2Format, 1)
      val leastDateDataASubtractDurationCol = leastDateUDF(substractDurationCol, dataAFormatted, Y4M2D2Format)
      val addDurationLeastDateCol = addDurationUDF(leastDateDataASubtractDurationCol, Y4M2D2Format, numeroMesi2)

      // SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING(ToString(AddDuration(...)), 0, 6)
      val dtRiferimentoAddDurationFilterCol = subtring06(tlburttFilter("dt_riferimento")) <=
        subtring06(addDurationLeastDateCol)

      cicliNdgDf.join(tlburttFilter, joinConditionCol)
        .filter(dtRIferimentoDataInizioDefFilterCol && dtRiferimentoAddDurationFilterCol)
        .select(cicliNdgDf("codicebanca"), cicliNdgDf("ndgprincipale"), cicliNdgDf("codicebanca_collegato"), cicliNdgDf("ndg_collegato"),
          cicliNdgDf("datainiziodef"), cicliNdgDf("datafinedef"), tlburttFilter("cd_istituto"), tlburttFilter("ndg"), tlburttFilter("sportello"),
          tlburttFilter("conto"), tlburttFilter("dt_riferimento"), tlburttFilter("conto_esteso"), tlburttFilter("forma_tecnica"),
          tlburttFilter("dt_accensione"), tlburttFilter("dt_estinzione"), tlburttFilter("dt_scadenza"), tlburttFilter("tp_ammortamento"),
          tlburttFilter("tp_rapporto"), tlburttFilter("period_liquid"), tlburttFilter("cd_prodotto_ris"), tlburttFilter("durata_originaria"),
          tlburttFilter("divisa"), tlburttFilter("durata_residua"), tlburttFilter("tp_contr_rapp"))
    })
      .reduce(_.union(_))
      .distinct()

    writeDataFrameAsCsvToPath(tlbcidefTlburtt, tlbcidefTlburttOutputPath)
  }

  private def subtring06(column: Column): Column = substring(toStringType(column), 0, 6)
}
