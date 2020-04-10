package it.carloni.luca.lgd.step

import it.carloni.luca.lgd.commons.LGDCommons
import it.carloni.luca.lgd.spark.AbstractSparkStep
import it.carloni.luca.lgd.schema.FrappNdgMonthlySchema
import it.carloni.luca.lgd.spark.utils.SparkUtils.{addDuration, leastDate, subtractDuration, toIntType, toStringType}
import it.carloni.luca.lgd.spark.utils.ScalaUtils.changeLocalDateFormat
import org.apache.spark.sql.functions.{col, substring}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.log4j.Logger

class FrappNdgMonthly(private val dataA: String, private val numeroMesi1: Int, private val numeroMesi2: Int)
  extends AbstractSparkStep {

  private val logger = Logger.getLogger(getClass)

  // STEP PATHS
  private val cicliNdgCsvPath = getPropertyValue("frapp.ndg.monthly.cicli.ndg.path.csv")
  private val tlburttCsvPath = getPropertyValue("frapp.ndg.monthly.tlburtt.csv")
  private val tlbcidefTlburttOutputPath = getPropertyValue("frapp.ndg.monthly.tlbcidef.tlburtt")

  // STEP SCHEMAS
  private val tlbcidefPigSchema = FrappNdgMonthlySchema.tlbcidefPigSchema
  private val tlburttPigSchema = FrappNdgMonthlySchema.tlburttPigSchema

  override def run(): Unit = {

    logger.debug(s"cicliNdgCsvPath: $cicliNdgCsvPath")
    logger.debug(s"tlburttCsvPath: $tlburttCsvPath")
    logger.debug(s"tlbcidefTlburttOutputPath: $tlbcidefTlburttOutputPath")
    logger.debug(s"dataA: $dataA")
    logger.debug(s"numeroMesi1: $numeroMesi1")
    logger.debug(s"numeroMesi2: $numeroMesi2")

    val tlbcidef = readCsvFromPathUsingSchema(cicliNdgCsvPath, tlbcidefPigSchema)
    val cicliNdgPrinc = tlbcidef.filter(col("cd_collegamento").isNull)
    val cicliNdgColl = tlbcidef.filter(col("cd_collegamento").isNotNull)

    // 60

    val tlburttFilter = readCsvFromPathUsingSchema(tlburttCsvPath, tlburttPigSchema)
      .filter(col("progr_segmento") === 0)

    // 111

    val Y4M2D2Format = "yyyyMMdd"
    val dataAFormatted = changeLocalDateFormat(dataA, LGDCommons.DatePatterns.DataAPattern, Y4M2D2Format)
    val tlbcidefTlburtt: DataFrame = Seq(cicliNdgPrinc, cicliNdgColl).map((cicliNdgDf: DataFrame) => {

      // JOIN cicli_ndg_princ BY (codicebanca_collegato, ndg_collegato), tlburtt_filter BY (cd_istituto, ndg);
      // OIN  cicli_ndg_coll BY (codicebanca_collegato, ndg_collegato), tlburtt_filter BY (cd_istituto, ndg);

      val joinConditionCol = (cicliNdgDf("codicebanca_collegato") === tlburttFilter("cd_istituto")) &&
        (cicliNdgDf("ndg_collegato") === tlburttFilter("ndg"))

      // FILTER BY ToDate((chararray)dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')

      val dtRIferimentoDataInizioDefFilterCol = tlburttFilter("dt_riferimento") >=
        toIntType(subtractDuration(cicliNdgDf("datainiziodef"), Y4M2D2Format, numeroMesi1))

      // SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M')
      // LeastDate( (int)ToString(SubtractDuration(....)
      // AddDuration( ToDate( (chararray) LeastDate(....)

      val substractDurationCol = subtractDuration(cicliNdgDf("datafinedef"), Y4M2D2Format, 1)
      val leastDateDataASubtractDurationCol = leastDate(substractDurationCol, dataAFormatted, Y4M2D2Format)
      val addDurationLeastDateCol = addDuration(leastDateDataASubtractDurationCol, Y4M2D2Format, numeroMesi2)

      // SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING(ToString(AddDuration(...)), 0, 6)
      val dtRiferimentoAddDurationFilterCol = toStringAndSubtring06(tlburttFilter("dt_riferimento")) <=
        toStringAndSubtring06(addDurationLeastDateCol)

      cicliNdgDf.join(tlburttFilter, joinConditionCol)
        .filter(dtRIferimentoDataInizioDefFilterCol && dtRiferimentoAddDurationFilterCol)
        .select()

    })
      .reduce(_.union(_))
      .distinct()

    writeDataFrameAsCsvToPath(tlbcidefTlburtt, tlbcidefTlburttOutputPath)
  }

  private def toStringAndSubtring06(column: Column): Column = substring(toStringType(column), 0, 6)
}
