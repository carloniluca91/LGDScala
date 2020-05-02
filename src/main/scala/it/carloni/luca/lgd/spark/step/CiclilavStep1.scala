package it.carloni.luca.lgd.spark.step

import it.carloni.luca.lgd.spark.utils.ScalaUtils.changeDateFormat
import it.carloni.luca.lgd.schema.CiclilavStep1Schema
import it.carloni.luca.lgd.scopt.config.DtDaDtAConfig
import it.carloni.luca.lgd.spark.common.AbstractSparkStep
import it.carloni.luca.lgd.spark.common.SparkEnums.DateFormats
import org.apache.spark.sql.functions.{coalesce, col, lit, max, min, trim, when}
import org.apache.log4j.Logger

import scala.collection.mutable

class CiclilavStep1 extends AbstractSparkStep[DtDaDtAConfig] {

  private val logger = Logger.getLogger(getClass)

  // STEP PATHS
  private val tlbcidefCsvPath = getPropertyValue("ciclilav.step1.tlbcidef.csv")
  private val tlbcraccCsvPath = getPropertyValue("ciclilav.step1.tlbcracc.csv")
  private val ciclilavStep1GenOutputPath = getPropertyValue("ciclilav.step1.out.csv")
  private val ciclilavStep1FileCraccOutputhPath = getPropertyValue("ciclilav.step1.filecracc")

  logger.info(s"ciclilav.step1.tlbcidef.csv: $tlbcidefCsvPath")
  logger.info(s"ciclilav.step1.tlbcracc.csv: $tlbcraccCsvPath")
  logger.info(s"ciclilav.step1.out.csv: $ciclilavStep1GenOutputPath")
  logger.info(s"ciclilav.step1.filecracc: $ciclilavStep1FileCraccOutputhPath")

  // STEP SCHEMAS
  private val tlbcidefPigSchema: mutable.LinkedHashMap[String, String] = CiclilavStep1Schema.tlbcidefPigSchema
  private val tlbcraccPigSchema: mutable.LinkedHashMap[String, String] = CiclilavStep1Schema.tlbcraccPigSchema

  def run(dataDaDataAConfig: DtDaDtAConfig): Unit = {

    logger.info(dataDaDataAConfig.toString)

    val dataDa: String = dataDaDataAConfig.dataDa
    val dataA: String = dataDaDataAConfig.dataA

    val tlbcidef = readCsvFromPathUsingSchema(tlbcidefCsvPath, tlbcidefPigSchema)

    /*
     (TRIM(status_ingresso)=='PASTDUE'?dt_ingresso_status:null) as datainiziopd,
     (TRIM(status_ingresso)=='INCA' or TRIM(status_ingresso)=='INADPRO'?dt_ingresso_status:null) as datainizioinc,
     (TRIM(status_ingresso)=='RISTR'?dt_ingresso_status:null) as datainizioristrutt,
     (TRIM(status_ingresso)=='SOFF'?dt_ingresso_status:null) as datainiziosoff
     */

    val statusIngressoColTrimCol = trim(col("status_ingresso"))
    val tlbcidefDataInizioPdCol = when(statusIngressoColTrimCol === "PASTDUE", col("dt_ingresso_status")).otherwise(null).as("datainiziopd")
    val tlbcidefDataInizioIncCol = when(statusIngressoColTrimCol.isin("INCA", "INADPRO"), col("dt_ingresso_status")).otherwise(null).alias("datainizioinc")
    val tlbcidefDataInizioRistruttCol = when(statusIngressoColTrimCol === "RISTR", col("dt_ingresso_status")).otherwise(null).as("datainizioristrutt")
    val tlbcidefDataInizioSoffCol = when(statusIngressoColTrimCol === "SOFF", col("dt_ingresso_status")).otherwise(null).as("datainiziosoff")

    // PARSE BOTH $data_da AND $data_a IN ORDER TO FIT WITH COLUMN dt_inizio_ciclo
    val Y4M2D2Format = DateFormats.Y4M2D2Format.toString
    val dataAFormat = DateFormats.DataAFormat.toString
    val dataDaInt: Int = changeDateFormat(dataDa, dataAFormat, Y4M2D2Format).toInt
    val dataAInt: Int = changeDateFormat(dataA, dataAFormat, Y4M2D2Format).toInt

    val tlbcidefUnpivot = tlbcidef
      .filter(col("dt_inizio_ciclo").between(dataDaInt, dataAInt))
      .select(col("cd_isti"), col("ndg_principale"), col("dt_inizio_ciclo"), col("dt_fine_ciclo"),
        tlbcidefDataInizioPdCol, tlbcidefDataInizioIncCol, tlbcidefDataInizioRistruttCol, tlbcidefDataInizioSoffCol)

    /*
      ,MAX(tlbcidef_unpivot.dt_fine_ciclo) as  dt_fine_ciclo
      ,MIN(tlbcidef_unpivot.datainiziopd) as datainiziopd
      ,MIN(tlbcidef_unpivot.datainizioristrutt) as datainizioristrutt
      ,MIN(tlbcidef_unpivot.datainizioinc) as datainizioinc
      ,MIN(tlbcidef_unpivot.datainiziosoff)  as datainiziosoff;
     */

    val tlbcidefMax = tlbcidefUnpivot
      .groupBy("cd_isti", "ndg_principale", "dt_inizio_ciclo")
      .agg(max(tlbcidefUnpivot("dt_fine_ciclo")).as("dt_fine_ciclo"),
        min(tlbcidefUnpivot("datainiziopd")).as("datainiziopd"),
        min(tlbcidefUnpivot("datainizioristrutt")).as("datainizioristrutt"),
        min(tlbcidefUnpivot("datainizioinc")).as("datainizioinc"),
        min(tlbcidefUnpivot("datainiziosoff")).as("datainiziosoff"))

    val dataALowerBound = if (dataAInt <= 20150731) 20150731 else dataAInt
    logger.info(s"dataALowerBound: $dataALowerBound")

    val tlbcracc = readCsvFromPathUsingSchema(tlbcraccCsvPath, tlbcraccPigSchema)
      .filter(col("data_rif") <= dataALowerBound)

    // CLONE TLBCRACC TO AVOID ANALYSIS EXCEPTION
    val tlbraccClone = tlbcracc.toDF()
      .withColumnRenamed("cd_isti", "cd_isti_clone")
      .withColumnRenamed("ndg", "ndg_clone")

    // JOIN tlbcidef_max BY (cd_isti, ndg_principale) LEFT, tlbcracc BY (cd_isti, ndg);
    val cicliRacc1JoinConditionCol = (tlbcidefMax("cd_isti") === tlbcracc("cd_isti")) && (tlbcidefMax("ndg_principale") === tlbcracc("ndg"))
    val cicliRacc1 = tlbcidefMax.join(tlbcracc, cicliRacc1JoinConditionCol, "left")
      .select(tlbcidefMax("cd_isti"), tlbcidefMax("ndg_principale"), tlbcidefMax("dt_inizio_ciclo"),
        tlbcidefMax("dt_fine_ciclo"), tlbcidefMax("datainiziopd"), tlbcidefMax("datainizioristrutt"),
        tlbcidefMax("datainizioinc"), tlbcidefMax("datainiziosoff"), tlbcracc("cod_raccordo"), tlbcracc("data_rif"))

    // (tlbcracc::cd_isti is not null ? tlbcracc::cd_isti : cicli_racc_1::cd_isti) as cd_isti_ced
    // (tlbcracc::ndg     is not null ? tlbcracc::ndg     : cicli_racc_1::ndg_principale) as ndg_ced
    // (tlbcracc::data_rif is not null ? tlbcracc::data_rif : cicli_racc_1::dt_inizio_ciclo) as dt_rif_cracc

    val cdIstiCedCol = coalesce(tlbraccClone("cd_isti_clone"), cicliRacc1("cd_isti")).as("cd_isti_ced")
    val ndgCedCol = coalesce(tlbraccClone("ndg_clone"), cicliRacc1("ndg_principale")).as("ndg_ced")
    val dtRifCraccCol = coalesce(tlbraccClone("data_rif"), cicliRacc1("dt_inizio_ciclo")).as("dt_rif_cracc")

    val ciclilavStep1Gen = cicliRacc1.join(tlbraccClone, Seq("cod_raccordo", "data_rif"), "left")
      .select(cicliRacc1("cd_isti"), cicliRacc1("ndg_principale"), cicliRacc1("dt_inizio_ciclo"), cicliRacc1("dt_fine_ciclo"),
        cicliRacc1("datainiziopd"), cicliRacc1("datainizioristrutt"), cicliRacc1("datainizioinc"), cicliRacc1("datainiziosoff"),
        lit(0).as("progr"), cdIstiCedCol, ndgCedCol)
      .distinct()

    val ciclilavStep1FileCracc = cicliRacc1.join(tlbraccClone, Seq("cod_raccordo", "data_rif"), "left")
      .select(cicliRacc1("cd_isti"), cicliRacc1("ndg_principale"), cicliRacc1("dt_inizio_ciclo"), cicliRacc1("dt_fine_ciclo"),
        cdIstiCedCol, ndgCedCol, dtRifCraccCol)
      .distinct()

    writeDataFrameAsCsvToPath(ciclilavStep1Gen, ciclilavStep1GenOutputPath)
    writeDataFrameAsCsvToPath(ciclilavStep1FileCracc, ciclilavStep1FileCraccOutputhPath)

  }
}
