package it.carloni.luca.lgd.steps

import it.carloni.luca.lgd.common.AbstractStep
import it.carloni.luca.lgd.schemas.SuppSoglSchema
import it.carloni.luca.lgd.common.utils.SparkUtils.{addDuration,
  isDateGeqOtherDate, isDateLeqOtherDate, leastDate,
  replaceAndToDouble, subtractDuration, toStringType}
import it.carloni.luca.lgd.common.utils.LGDCommons
import it.carloni.luca.lgd.common.utils.JavaUtils.changeLocalDateFormat
import it.carloni.luca.lgd.options.OptionNaming
import org.apache.spark.sql.functions.{col, substring, sum}
import org.apache.log4j.Logger

import scala.collection.mutable

class SuppSogl(stepParamsMap: mutable.Map[String, String])
  extends AbstractStep{

  private val logger = Logger.getLogger(getClass)
  private val cicliS1PigSchema = SuppSoglSchema.cicliS1PigSchema
  private val tlbudctLoadPigSchema = SuppSoglSchema.tlbudctLoadPigSchema
  private val tlburdaLoadPigSchema = SuppSoglSchema.tlburdaLoadPigSchema
  private val tlbucocLoadPigSchema = SuppSoglSchema.tlbucocLoadPigSchema

  override def run(){

    val cicliS1Path = getValue("supp_sogl.input.table.cicli_s1")
    val tlbudctLoadPath = getValue("supp_sogl.input.table.tlbudct_load")
    val tlburdaLoadPath = getValue("supp_sogl.input.table.tlburda_load")
    val tlbucocLoadPath = getValue("supp_sogl.input.table.tlbucoc_load")
    val fileOutPath = getValue("supp_sogl.data.output.dir")

    logger.debug(f"cicliS1Path: $cicliS1Path")
    logger.debug(f"tlbudctLoadPath: $tlbudctLoadPath")
    logger.debug(f"tlburdaLoadPath: $tlburdaLoadPath")
    logger.debug(f"tlbucocLoadPath: $tlbucocLoadPath")
    logger.debug(f"fileOutPath: $fileOutPath")

    val numeroMesi1 = stepParamsMap(OptionNaming.NumeroMesi1LongOption).toInt
    val numeroMesi2 = stepParamsMap(OptionNaming.NumeroMesi2LongOption).toInt
    val dataA = stepParamsMap(OptionNaming.DataALongOption)
    val dataAPattern = LGDCommons.DatePatterns.DataAPattern

    logger.debug(f"numeroMesi1: $numeroMesi1")
    logger.debug(f"numeroMesi2: $numeroMesi2")
    logger.debug(f"dataA: $dataA")
    logger.debug(f"dataAPattern: $dataAPattern")

    // 21

    val cicliS1StructType = fromPigSchemaToStructType(cicliS1PigSchema)
    val cicliS1 = readCsvFromPathUsingSchema(cicliS1Path, cicliS1StructType)

    val tlbudctLoadStructType = fromPigSchemaToStructType(tlbudctLoadPigSchema)
    val tlbudct = readCsvFromPathUsingSchema(tlbudctLoadPath, tlbudctLoadStructType)
      .select("cd_istituto", "ndg", "dt_riferimento", "status_ndg", "posiz_soff_inc", "cd_rap_ristr", "tp_cli_scad_scf")

    // 79

    // 84
    // JOIN cicli_s1  BY (cd_isti_ced, ndg_ced ),tlbudct   BY (cd_istituto, ndg );
    val cicliUdctJoinCondition = (cicliS1("cd_isti_ced") === tlbudct("cd_istituto")) && (cicliS1("ndg_ced") === tlbudct("ndg"))

    // FILTER cicli_s1_tlbudct_join
    // BY ToDate((chararray)dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate((chararray)dt_inizio_ciclo,'yyyyMMdd'),'$numero_mesi_1')
    val subtractDurationDtInizioCicloCol = subtractDuration(toStringType(cicliS1("dt_inizio_ciclo")), "yyyyMMdd", numeroMesi1)
    val dtRiferimentoDtInizioCicloCicliUdctFilterCol = isDateGeqOtherDate(toStringType(tlbudct("dt_riferimento")), "yyyyMMdd",
      subtractDurationDtInizioCicloCol, "yyyyMMdd")

    // AND SUBSTRING( (chararray)dt_riferimento,0,6 ) <= ...
    val dtRiferimentoSubstringCol = substring(toStringType(tlbudct("dt_riferimento")), 0, 6)

    // ...<= SUBSTRING(ToString(AddDuration(ToDate( (chararray)LeastDate( (int)ToString(
    // SubtractDuration(ToDate((chararray)dt_fine_ciclo,'yyyyMMdd' ),'P1M'),'yyyyMMdd'), $data_a ),'yyyyMMdd' ),'$numero_mesi_2' ),'yyyyMMdd'),0,6 )
    val dtFineCicloSubtractDurationCol = subtractDuration(toStringType(cicliS1("dt_fine_ciclo")), "yyyyMMdd", 1)

    // ADJUST FORMAT OF $data_a IN ORDER TO FIT WITH LEASTDATEUDF
    val dataAFormatted = changeLocalDateFormat(dataA, dataAPattern, "yyyyMMdd")
    val dtFineCicloSubtractDurationLeastDateCol = leastDate(dtFineCicloSubtractDurationCol, dataAFormatted, "yyyyMMdd")
    val leastDateAddDurationDtFineCicloCol = addDuration(dtFineCicloSubtractDurationLeastDateCol, "yyyyMMdd", numeroMesi2)
    val substringLeastDateAddDurationDtFineCicloCol = substring(leastDateAddDurationDtFineCicloCol, 0, 6)
    val dtRiferimentoSubstringColLeqSubstringLeastDateColFilterCol = isDateLeqOtherDate(dtRiferimentoSubstringCol, "yyyyMMdd",
      substringLeastDateAddDurationDtFineCicloCol, "yyyyMMdd")

    val cicliUdct = cicliS1.join(tlbudct, cicliUdctJoinCondition)
      .filter(dtRiferimentoDtInizioCicloCicliUdctFilterCol && dtRiferimentoSubstringColLeqSubstringLeastDateColFilterCol)
      .select(cicliS1("cd_isti"), cicliS1("ndg_principale"), cicliS1("dt_inizio_ciclo"), cicliS1("dt_fine_ciclo"), cicliS1("datainiziopd"),
        cicliS1("datainizioristrutt"), cicliS1("datainizioinc"), cicliS1("datainiziosoff"), cicliS1("progr"), cicliS1("cd_isti_ced"),
        cicliS1("ndg_ced"), tlbudct("dt_riferimento").alias("dt_riferimento_udct"), tlbudct("status_ndg"), tlbudct("posiz_soff_inc"),
        tlbudct("cd_rap_ristr"), tlbudct("tp_cli_scad_scf"))

    // 111

    // 116

    val tlburdaLoadStructType = fromPigSchemaToStructType(tlburdaLoadPigSchema)
    val tlburdaLoad = readCsvFromPathUsingSchema(tlburdaLoadPath, tlburdaLoadStructType)
      .select("cd_istituto", "ndg", "dt_riferimento", "progr_segmento", "imp_accordato", "imp_utilizzo_cassa")

    val tlburdaFilter = tlburdaLoad
      .withColumn("imp_accordato", replaceAndToDouble(tlburdaLoad("imp_accordato"), ",", "."))
      .withColumn("imp_utilizzo_cassa", replaceAndToDouble(tlburdaLoad("imp_utilizzo_cassa"), ",", "."))
      .filter(tlburdaLoad("progr_segmento") === 0)

    val tlburda = tlburdaFilter.groupBy("cd_istituto", "ndg", "dt_riferimento")
      .agg(sum(tlburdaFilter("imp_accordato").alias("imp_accordato")),
        sum(tlburdaFilter("imp_utilizzo_cassa").alias("imp_utilizzo_cassa")))

    // 190

    // 192

    // JOIN cicli_udct  BY (cd_isti_ced, ndg_ced, dt_riferimento_udct ) LEFT ,tlburda     BY (cd_istituto, ndg, dt_riferimento );
    val cicliUdctUrdaJoinCondition = (cicliUdct("cd_isti_ced") === tlburda("cd_istituto")) &&
      (cicliUdct("ndg_ced") === tlburda("ndg")) && (cicliUdct("dt_riferimento_udct") === tlburda("dt_riferimento"))

    val cicliUdctUrda = cicliUdct.join(tlburda, cicliUdctUrdaJoinCondition, "left")
      .select(cicliUdct("cd_isti"), cicliUdct("ndg_principale"), cicliUdct("dt_inizio_ciclo"), cicliUdct("dt_fine_ciclo"),
        cicliUdct("datainiziopd"), cicliUdct("datainizioristrutt"), cicliUdct("datainizioinc"), cicliUdct("datainiziosoff"),
        cicliUdct("progr"), cicliUdct("cd_isti_ced"), cicliUdct("ndg_ced"), cicliUdct("dt_riferimento_udct"),
        cicliUdct("status_ndg"), cicliUdct("posiz_soff_inc"), cicliUdct("cd_rap_ristr"), cicliUdct("tp_cli_scad_scf"),
        tlburda("dt_riferimento").alias("dt_riferimento_urda"),
        tlburda("imp_utilizzo_cassa").alias("util_cassa"),
        tlburda("imp_accordato"))

    // 215

    val tlbucocLoadStructType = fromPigSchemaToStructType(tlbucocLoadPigSchema)
    val tlbucocFilter = readCsvFromPathUsingSchema(tlbucocLoadPath, tlbucocLoadStructType)
      .select("cd_istituto", "ndg", "dt_riferimento", "progr_segmento", "addebiti_in_sosp")
      .withColumn("addebiti_in_sosp", replaceAndToDouble(col("addebiti_in_sosp"), ",", "."))
      .filter(col("progr_segmento") === 0)

    // 250

    // 252

    val tlbucoc = tlbucocFilter.groupBy("cd_istituto", "ndg", "dt_riferimento")
      .agg(sum(tlbucocFilter("addebiti_in_sosp")).alias("addebiti_in_sosp"))

    // 260

    // 261

    // JOIN cicli_udct_urda  BY (cd_isti_ced, ndg_ced, dt_riferimento_udct ) LEFT, ,tlbucoc BY (cd_istituto, ndg, dt_riferimento );
    val fileOutJoinCondition = (cicliUdctUrda("cd_isti_ced") === tlbucoc("cd_istituto")) &&
      (cicliUdctUrda("ndg_ced") === tlbucoc("ndg")) &&
      (cicliUdctUrda("dt_riferimento_udct") === tlbucoc("dt_riferimento"))

    val fileOut = cicliUdctUrda.join(tlbucoc, fileOutJoinCondition, "left")
      .select(cicliUdctUrda("cd_isti"), cicliUdctUrda("ndg_principale"), cicliUdctUrda("dt_inizio_ciclo"),
        cicliUdctUrda("dt_fine_ciclo"), cicliUdctUrda("datainiziopd"), cicliUdctUrda("datainizioristrutt"), cicliUdctUrda("datainizioinc"),
        cicliUdctUrda("datainiziosoff"), cicliUdctUrda("progr"), cicliUdctUrda("cd_isti_ced"), cicliUdctUrda("ndg_ced"),
        cicliUdctUrda("dt_riferimento_udct"), cicliUdctUrda("status_ndg"), cicliUdctUrda("posiz_soff_inc"), cicliUdctUrda("cd_rap_ristr"),
        cicliUdctUrda("tp_cli_scad_scf"), cicliUdctUrda("dt_riferimento_urda"), cicliUdctUrda("util_cassa"), cicliUdctUrda("imp_accordato"),
        tlbucoc("addebiti_in_sosp").alias("tot_add_sosp"))

    // 288

    writeDataFrameAsCsvToPath(fileOut, fileOutPath)
  }
}
