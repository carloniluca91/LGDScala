package it.carloni.luca.lgd.spark.step

import it.carloni.luca.lgd.schema.FrappPumaSchema
import it.carloni.luca.lgd.scopt.config.DtAConfig
import it.carloni.luca.lgd.spark.common.{AbstractSparkStep, SparkEnums}
import it.carloni.luca.lgd.spark.utils.SparkUtils.{leastDateUDF, subtractDurationUDF, toStringType}
import it.carloni.luca.lgd.spark.utils.ScalaUtils.changeDateFormat
import org.apache.spark.sql.functions.{col, substring}
import org.apache.spark.sql.Column
import org.apache.log4j.Logger

import scala.collection.mutable

class FrappPuma extends AbstractSparkStep[DtAConfig] {

  private val logger = Logger.getLogger(getClass)

  // STEP PATHS
  private val cicliNdgCsvPath = getPropertyValue("frapp.puma.cicli.ndg.path")
  private val tlbgaranCsvPath = getPropertyValue("frapp.puma.tlbgaran.path")
  private val frappPumaOutputPath = getPropertyValue("frapp.puma.frapp.puma.out")

  // STEP SCHEMAS
  private val tlbcidefPigSchema:  mutable.LinkedHashMap[String, String] = FrappPumaSchema.tlbcidefPigSchema
  private val tlbgaranPigSchema: mutable.LinkedHashMap[String, String] = FrappPumaSchema.tlbgaranPigSchema

  override def run(dtAConfig: DtAConfig): Unit = {

    val dataA: String = dtAConfig.dataA

    logger.info(dtAConfig.toString)

    logger.info(s"frapp.puma.cicli.ndg.path: $cicliNdgCsvPath")
    logger.info(s"frapp.puma.tlbgaran.path: $tlbgaranCsvPath")
    logger.info(s"frapp.puma.frapp.puma.out: $frappPumaOutputPath")

    val tlbcidef = readCsvFromPathUsingSchema(cicliNdgCsvPath, tlbcidefPigSchema)
    val cicliNdgPrinc = tlbcidef.filter(col("cd_collegamento").isNull)
    val cicliNdgColl = tlbcidef.filter(col("cd_collegamento").isNotNull)

    val tlbgaran = readCsvFromPathUsingSchema(tlbgaranCsvPath, tlbgaranPigSchema)

    // 71

    // JOIN  tlbgaran BY (cd_istituto, ndg), cicli_ndg_princ BY (codicebanca_collegato, ndg_collegato);
    val tlbcidefTlbgaranPrincJoinCondition = (tlbgaran("cd_istituto") === cicliNdgPrinc("ndg")) &&
      (tlbgaran("codicebanca_collegato") === cicliNdgPrinc("ndg_collegato"))

    // FILTER BY ToDate( (chararray)dt_riferimento,'yyyyMMdd') >= ToDate( (chararray)datainiziodef,'yyyyMMdd' )
    // and SUBSTRING( (chararray)dt_riferimento,0,6 ) <= SUBSTRING( (chararray)LeastDate( (int)ToString(SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M'),'yyyyMMdd') ,$data_a),0,6 );

    // FORMAT $data_a IN ORDER TO FIT WITH LEAST_DATE FUNCTION
    val dataAPattern: String = SparkEnums.DateFormats.DataAFormat.toString
    val YYYYMMDDFormat: String = SparkEnums.DateFormats.Y4M2D2Format.toString
    val dataAFormatted = changeDateFormat(dataA, dataAPattern, YYYYMMDDFormat)
    val dataFineDefSubtractDurationCol = subtractDurationUDF(toStringType(cicliNdgPrinc("datainiziodef")), YYYYMMDDFormat, 1)
    val leastDateSubtractDurationCol = leastDateUDF(dataFineDefSubtractDurationCol, dataAFormatted, YYYYMMDDFormat)

    val tlbcidefTlbgaranPrincFilterCondition = (tlbgaran("dt_riferimento") >= cicliNdgPrinc("datainiziodef")) &&
      (toStringAndSubstring(tlbgaran("dt_riferimento")) <= toStringAndSubstring(leastDateSubtractDurationCol))

    val tlbcidefTlbgaranPrinc = tlbgaran.join(cicliNdgPrinc, tlbcidefTlbgaranPrincJoinCondition)
      .filter(tlbcidefTlbgaranPrincFilterCondition)
      .select(tlbgaran("cd_istituto").as("cd_isti"), tlbgaran("ndg"), tlbgaran("sportello"), tlbgaran("dt_riferimento"),
        tlbgaran("conto_esteso"), tlbgaran("cd_puma2"), tlbgaran("ide_garanzia"), tlbgaran("importo"), tlbgaran("fair_value"),
        cicliNdgPrinc("codicebanca"), cicliNdgPrinc("ndgprincipale"), cicliNdgPrinc("datainiziodef"))

    // 102

    // JOIN  tlbgaran BY (cd_istituto, ndg, dt_riferimento), cicli_ndg_coll BY (codicebanca_collegato, ndg_collegato, dt_rif_udct);
    val tlbcidefTlbgaranCollJoinCondition = (tlbgaran("cd_istituto") === cicliNdgColl("codicebanca_collegato")) &&
      (tlbgaran("ndg") === cicliNdgColl("ndg_collegato")) &&
      (tlbgaran("dt_riferimento") === cicliNdgColl("dt_rif_udct"))

    val tlbcidefTlbgaranColl = tlbgaran.join(cicliNdgColl, tlbcidefTlbgaranCollJoinCondition)
      .select(tlbgaran("cd_istituto").as("cd_isti"), tlbgaran("ndg"), tlbgaran("sportello"), tlbgaran("dt_riferimento"),
        tlbgaran("conto_esteso"), tlbgaran("cd_puma2"), tlbgaran("ide_garanzia"), tlbgaran("importo"), tlbgaran("fair_value"),
        cicliNdgColl("codicebanca"), cicliNdgColl("ndgprincipale"), cicliNdgColl("datainiziodef"))

    // 118

    val cicliAll = tlbcidefTlbgaranPrinc
      .union(tlbcidefTlbgaranColl)
      .distinct

    writeDataFrameAsCsvToPath(cicliAll, frappPumaOutputPath)
  }

  private def toStringAndSubstring(column: Column): Column = substring(toStringType(column), 0, 6)
}
