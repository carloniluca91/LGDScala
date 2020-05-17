package it.carloni.luca.lgd.spark.step

import it.carloni.luca.lgd.schema.PosaggrSchema
import it.carloni.luca.lgd.scopt.config.NoValueConfig
import it.carloni.luca.lgd.spark.common.AbstractSparkStep
import org.apache.log4j.Logger
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{col, regexp_replace, sum, trim}
import org.apache.spark.sql.types.DataTypes

import scala.collection.mutable

class Posaggr extends AbstractSparkStep[NoValueConfig] {

  private val logger = Logger.getLogger(getClass)

  // STEP PATHS
  private val tblcompCsvPath = getPropertyValue("posaggr.tblcomp.path.csv")
  private val tlbaggrCsvPath = getPropertyValue("posaggr.tlbaggr.path.csv")
  private val tlbposiCsvPath = getPropertyValue("posaggr.tlbposi.load.csv")
  private val posaggrOutputPath = getPropertyValue("posaggr.out.csv")

  // STEP SCHEMAS
  private val tblcompPigSchema: mutable.LinkedHashMap[String, String] = PosaggrSchema.tblcompPigSchema
  private val tlbaggrPigSchema: mutable.LinkedHashMap[String, String] = PosaggrSchema.tlbaggrPigSchema
  private val tlbposiPigSchema: mutable.LinkedHashMap[String, String] = PosaggrSchema.tlbposiPigSchema

  override def run(noValueConfig: NoValueConfig): Unit = {

    logger.info(s"posaggr.tblcomp.path.csv: $tblcompCsvPath")
    logger.info(s"posaggr.tlbaggr.path.csv: $tlbaggrCsvPath")
    logger.info(s"posaggr.tlbposi.load.csv: $tlbposiCsvPath")
    logger.info(s"posaggr.out.csv: $posaggrOutputPath")

    val tblcomp = readCsvFromPathUsingSchema(tblcompCsvPath, tblcompPigSchema)
    val tlbaggr = readCsvFromPathUsingSchema(tlbaggrCsvPath, tlbaggrPigSchema)

    // 59

    // JOIN tlbaggr BY (dt_riferimento,c_key_aggr,tipo_segmne_aggr,cd_istituto), tblcomp BY ( dt_riferimento, c_key, tipo_segmne, cd_istituto);
    val tblcompTlbaggrJoinCondition = (tlbaggr("dt_riferimento") === tblcomp("dt_riferimento")) &&
      (tlbaggr("c_key_aggr") === tblcomp("c_key")) &&
      (tlbaggr("tipo_segmne_aggr") === tblcomp("tipo_segmne")) &&
      (tlbaggr("cd_istituto") === tblcomp("cd_istituto"))

    val tblcompTlbaggr = tlbaggr.join(tblcomp, tblcompTlbaggrJoinCondition)
      .select(tlbaggr("dt_riferimento"), tblcomp("cd_istituto"), tblcomp("ndg"), tlbaggr("c_key_aggr"), tlbaggr("tipo_segmne_aggr"),
        tlbaggr("segmento"), tlbaggr("tp_ndg"))

    // 79

    val tlbposi = readCsvFromPathUsingSchema(tlbposiCsvPath, tlbposiPigSchema)
      .withColumn("bo_acco", replaceCommaAndToDouble(col("bo_acco")))
      .withColumn("bo_util", replaceCommaAndToDouble(col("bo_util")))
      .withColumn("tot_add_sosp", replaceCommaAndToDouble(col("tot_add_sosp")))
      .withColumn("tot_val_intr", replaceCommaAndToDouble(col("tot_val_intr")))
      .withColumn("ca_acco", replaceCommaAndToDouble(col("ca_acco")))
      .withColumn("ca_util", replaceCommaAndToDouble(col("ca_util")))
      .withColumn("util_cassa", replaceCommaAndToDouble(col("util_cassa")))
      .withColumn("fido_op_cassa", replaceCommaAndToDouble(col("fido_op_cassa")))
      .withColumn("utilizzo_titoli", replaceCommaAndToDouble(col("utilizzo_titoli")))
      .withColumn("esposizione_titoli", replaceCommaAndToDouble(col("esposizione_titoli")))

    // 140

    // JOIN tblcomp_tlbaggr BY (dt_riferimento,cd_istituto,ndg), tlbposi BY (dt_riferimento,cd_istituto,ndg);
    val tblcompTlbaggrTlbposi = tblcompTlbaggr.join(tlbposi, Seq("dt_riferimento", "cd_istituto", "ndg"))
      .select(tblcompTlbaggr("dt_riferimento"), tblcompTlbaggr("cd_istituto"), tblcompTlbaggr("ndg"), tblcompTlbaggr("c_key_aggr"),
        tblcompTlbaggr("tipo_segmne_aggr"), tblcompTlbaggr("segmento"), trim(tblcompTlbaggr("tp_ndg")).as("tp_ndg"), tlbposi("bo_acco"),
        tlbposi("bo_util"), tlbposi("tot_add_sosp"), tlbposi("tot_val_intr"), tlbposi("ca_acco"), tlbposi("ca_util"), tlbposi("util_cassa"),
        tlbposi("fido_op_cassa"), tlbposi("utilizzo_titoli"), tlbposi("esposizione_titoli"))

    // 167

    // GROUP tblcomp_tlbaggr_tlbposi BY (dt_riferimento, cd_istituto, c_key_aggr, tipo_segmne_aggr);
    val posaggrWindowSpec: WindowSpec = Window.partitionBy("dt_riferimento", "cd_istituto", "c_key_aggr", "tipo_segmne_aggr")
    val posaggr = tblcompTlbaggrTlbposi
      .select(col("dt_riferimento"), col("cd_istituto"), col("c_key_aggr"),
        col("tipo_segmne_aggr"), col("segmento"), col("tp_ndg"),
        computeSumOverWindow(col("bo_acco"), posaggrWindowSpec, "accordato_bo"),
        computeSumOverWindow(col("bo_util"), posaggrWindowSpec, "utilizzato_bo"),
        computeSumOverWindow(col("tot_add_sosp"), posaggrWindowSpec, "tot_add_sosp"),
        computeSumOverWindow(col("tot_val_intr"), posaggrWindowSpec, "tot_val_intr_ps"),
        computeSumOverWindow(col("ca_acco"), posaggrWindowSpec, "accordato_ca"),
        computeSumOverWindow(col("ca_util"), posaggrWindowSpec, "utilizzato_ca"),
        computeSumOverWindow(col("util_cassa"), posaggrWindowSpec, "util_cassa"),
        computeSumOverWindow(col("fido_op_cassa"), posaggrWindowSpec, "fido_op_cassa"),
        computeSumOverWindow(col("utilizzo_titoli"), posaggrWindowSpec, "utilizzo_titoli"),
        computeSumOverWindow(col("esposizione_titoli"), posaggrWindowSpec, "esposizione_titoli"))

    writeDataFrameAsCsvToPath(posaggr, posaggrOutputPath)
  }

  private def replaceCommaAndToDouble(column: Column): Column  =
    regexp_replace(column, ",", ".").cast(DataTypes.DoubleType)

  private def computeSumOverWindow(column: Column, windowSpec: WindowSpec, alias: String): Column =
    sum(column).over(windowSpec).cast(DataTypes.DoubleType).as(alias)
}
