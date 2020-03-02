package it.carloni.luca.lgd.steps

import it.carloni.luca.lgd.common.AbstractStep
import it.carloni.luca.lgd.common.utils.{SparkUtils, LGDCommons}
import it.carloni.luca.lgd.options.OptionNaming
import it.carloni.luca.lgd.schemas.CicliLavStep2Schema
import org.apache.spark.sql.functions.{col, lit, substring, sum}
import org.apache.log4j.Logger

import scala.collection.mutable

class CicliLavStep2(stepParamsMap: mutable.Map[String, String])
  extends AbstractStep {

  private val logger = Logger.getLogger(getClass)
  private val tlbcidefMax1Schema = CicliLavStep2Schema.tlbcidefMax1PigSchema
  private val tlbucolLoadSchema = CicliLavStep2Schema.tlbucolLoadPigSchema
  private val tlburdaSchema = CicliLavStep2Schema.tlburdaLoadPigSchema
  private val tlbudctSchema = CicliLavStep2Schema.tlbudctLoadPigSchema
  private val tlbucocSchema = CicliLavStep2Schema.tlbucocLoadPigSchema

  override def run(){

    val periodo = stepParamsMap(OptionNaming.PeriodoLongOption)
    val periodoPattern = LGDCommons.DatePatterns.PeriodoPattern
    val tlbcidefMax1Path = getValue("ciclilavstep2.input.table.tlbcidef_max1")
    val tlbucolLoadPath = getValue("ciclilavstep2.input.table.tlbucol_load")
    val tlbudctLoadPath = getValue("ciclilavstep2.input.table.tlbudct_load")
    val tlburdaLoadPath = getValue("ciclilavstep2.input.table.tlburda_load")
    val tlbucocLoadPath = getValue("ciclilavstep2.input.table.tlbucoc_load")
    val cicliLavStep2OutDirPath = getValue("ciclilavstep2.data.output.dir")

    logger.debug(f"periodo: $periodo")
    logger.debug(f"periodoPattern: $periodoPattern")
    logger.debug(f"tlbcidefMax1Path: $tlbcidefMax1Path")
    logger.debug(f"tlbucolLoadPath: $tlbucolLoadPath")
    logger.debug(f"tlbudctLoadPath: $tlbudctLoadPath")
    logger.debug(f"tlburdaLoadPath: $tlburdaLoadPath")
    logger.debug(f"tlbucocLoadPath: $tlbucocLoadPath")
    logger.debug(f"cicliLavStep2OutDirPath: $cicliLavStep2OutDirPath")

    // 27

    /*
    tlbcidef_max = FILTER tlbcidef_max1 BY SUBSTRING((chararray)dt_inizio_ciclo,0,6) >= SUBSTRING('$periodo',0,6)
    AND SUBSTRING((chararray)dt_inizio_ciclo,0,6) <=
    ( SUBSTRING('$periodo',7,13) is null ? SUBSTRING('$periodo',0,6) : SUBSTRING('$periodo',7,13) );
        */

    val tlbcidefMax1StructTypeSchema = fromPigSchemaToStructType(tlbcidefMax1Schema)
    val tlbcidefMax1 = readCsvFromPathUsingSchema(tlbcidefMax1Path, tlbcidefMax1StructTypeSchema)

    val dtInizioCicloSubstringCol = substring(tlbcidefMax1("dt_inizio_ciclo"), 0, 7)
    val substringPeriodo07 = periodo.substring(0, 7)
    val substringPeriodo714 = if ((periodo.length < 14) && (periodo.length > 7)) substringPeriodo07 else periodo.substring(7, 14)

    val dtInizioCicloGeqPeriodoFilterCol = SparkUtils.isDateGeqOtherDate(dtInizioCicloSubstringCol, periodoPattern, lit(substringPeriodo07), periodoPattern)
    val dtInizioCicloLeqPeriodoFilterCol = SparkUtils.isDateLeqOtherDate(dtInizioCicloSubstringCol, periodoPattern, lit(substringPeriodo714), periodoPattern)
    val tlbcidefMax = tlbcidefMax1.filter(dtInizioCicloGeqPeriodoFilterCol && dtInizioCicloLeqPeriodoFilterCol)

    val tlbucolSchema = fromPigSchemaToStructType(tlbucolLoadSchema)
    val tlbucolFiltered = readCsvFromPathUsingSchema(tlbucolLoadPath, tlbucolSchema)
      .filter(col("cd_collegamento").isin("103", "105", "106", "107", "207"))

    // 68

    /*
    cidef_ucol_join_coll =
    JOIN  tlbcidef_max BY (cd_isti_ced, ndg_ced, dt_riferimento_udct)
    ,tlbucol_filtered BY (cd_istituto, ndg_collegato, dt_riferimento);
     */

    val cidefUcollCollJoinCondition = ((tlbcidefMax("cd_isti_ced") === tlbucolFiltered("cd_istituto"))
      && (tlbcidefMax("ndg_ced") === tlbucolFiltered("ndg_collegato"))
      && (tlbcidefMax("dt_riferimento_udct") === tlbucolFiltered("dt_riferimento")))

    val cidefUcollColl = tlbcidefMax.join(tlbucolFiltered, cidefUcollCollJoinCondition)
      .select(tlbcidefMax("cd_isti"), tlbcidefMax("ndg_principale"), tlbcidefMax("dt_inizio_ciclo"), tlbcidefMax("dt_fine_ciclo"),
        tlbcidefMax("datainiziopd"), tlbcidefMax("datainizioristrutt"), tlbcidefMax("datainizioinc"), tlbcidefMax("datainiziosoff"),
        tlbcidefMax("progr"),
        tlbucolFiltered("cd_istituto").alias("cd_isti_coll"), tlbucolFiltered("ndg").alias("ndg_coll"),
        tlbucolFiltered("dt_riferimento").alias("dt_riferimento_ucol"), tlbucolFiltered("cd_collegamento"))

    // 89

    /*
    cidef_ucol_join_ndg =
    JOIN  tlbcidef_max BY (cd_isti_ced, ndg_ced, dt_riferimento_udct)
    ,tlbucol_filtered BY (cd_istituto, ndg, dt_riferimento);
     */

    val cidefUcolNdgJoinCondition = ((tlbcidefMax("cd_isti_ced") === tlbucolFiltered("cd_istituto"))
      && (tlbcidefMax("ndg_ced") === tlbucolFiltered("ndg"))
      && (tlbcidefMax("dt_riferimento_udct") === tlbucolFiltered("dt_riferimento")))

    val cidefUcollNdg = tlbcidefMax.join(tlbucolFiltered, cidefUcolNdgJoinCondition)
      .select(tlbcidefMax("cd_isti"), tlbcidefMax("ndg_principale"), tlbcidefMax("dt_inizio_ciclo"), tlbcidefMax("dt_fine_ciclo"),
        tlbcidefMax("datainiziopd"), tlbcidefMax("datainizioristrutt"), tlbcidefMax("datainizioinc"), tlbcidefMax("datainiziosoff"),
        tlbcidefMax("progr"),
        tlbucolFiltered("cd_istituto").alias("cd_isti_coll"), tlbucolFiltered("ndg_collegato").alias("ndg_coll"),
        tlbucolFiltered("dt_riferimento").alias("dt_riferimento_ucol"), tlbucolFiltered("cd_collegamento"))

    val cicliUcol = cidefUcollColl.union(cidefUcollNdg)

    // 132

    val tlbudctStructTypeSchema = fromPigSchemaToStructType(tlbudctSchema)
    val tlbudct = readCsvFromPathUsingSchema(tlbudctLoadPath, tlbudctStructTypeSchema)

    // JOIN cicli_ucol  BY ( cd_isti_coll, ndg_coll, dt_riferimento_ucol )
    // ,tlbudct         BY ( cd_istituto, ndg, dt_riferimento );

    // 176

    val cicliUcollUdctJoinCondition = ((cicliUcol("cd_isti_coll") === tlbudct("cd_istituto"))
      && (cicliUcol("ndg_coll") === tlbudct("ndg"))
      && (cicliUcol("dt_riferimento_ucol") === tlbudct("dt_riferimento")))

    val cicliUcolUdct = cicliUcol.join(tlbudct, cicliUcollUdctJoinCondition)
      .select(cicliUcol("cd_isti"), cicliUcol("ndg_principale"), cicliUcol("dt_inizio_ciclo"), cicliUcol("dt_fine_ciclo"), cicliUcol("datainiziopd"),
        cicliUcol("datainizioristrutt"), cicliUcol("datainizioinc"), cicliUcol("datainiziosoff"), cicliUcol("progr"), cicliUcol("cd_isti_coll"),
        cicliUcol("ndg_coll"), cicliUcol("dt_riferimento_ucol"), cicliUcol("cd_collegamento"),
        tlbudct("dt_riferimento").alias("dt_riferimento_udct"), tlbudct("status_ndg"), tlbudct("posiz_soff_inc"), tlbudct("cd_rap_ristr"),
        tlbudct("tp_cli_scad_scf"))

    // 206

    val tlburdaStructTypeSchema = fromPigSchemaToStructType(tlburdaSchema)
    val tlburdaFilter = readCsvFromPathUsingSchema(tlburdaLoadPath, tlburdaStructTypeSchema)
      .filter(col("progr_segmento") === 0)
      .withColumn("imp_accordato", SparkUtils.replaceAndToDouble(col("imp_accordato"), ",", "."))
      .withColumn("imp_utilizzo_cassa", SparkUtils.replaceAndToDouble(col("imp_utilizzo_cassa"), ",", "."))

    val tlburda = tlburdaFilter.groupBy(col("cd_istituto"), col("ndg"), col("dt_riferimento"))
      .agg(sum("imp_accordato").alias("imp_accordato"), sum("imp_utilizzo_cassa").alias("imp_utilizzo_cassa"))


    // 283

    // JOIN cicli_ucol_udct  BY (cd_isti_coll, ndg_coll, dt_riferimento_ucol ) LEFT,
    //      tlburda          BY (cd_istituto, ndg, dt_riferimento );

    val cicliUcolUdctUrdaJoinCondition = ((cicliUcolUdct("cd_isti_coll") === tlburda("cd_istituto"))
      && (cicliUcolUdct("ndg_coll") === tlburda("ndg"))
      && (cicliUcolUdct("dt_riferimento_ucol") === tlburda("dt_riferimento")))

    val cicliUcolUdctUrda = cicliUcolUdct.join(tlburda, cicliUcolUdctUrdaJoinCondition, "left_outer")
      .select(cicliUcolUdct("*"), tlburda("dt_riferimento").alias("dt_riferimento_urda"),
        tlburda("imp_utilizzo_cassa").alias("util_cassa"), tlburda("imp_accordato"))

    // 311

    val tlbucocStructTypeSchema = fromPigSchemaToStructType(tlbucocSchema)
    val tlbucocFilter = readCsvFromPathUsingSchema(tlburdaLoadPath, tlbucocStructTypeSchema)
      .filter(col("progr_segmento") === 0)
      .withColumn("addebiti_in_sosp", SparkUtils.replaceAndToDouble(col("addebiti_in_sosp"), ",", "."))

    val tlbucoc = tlbucocFilter.groupBy(col("cd_istituto"), col("ndg"), col("dt_riferimento"))
      .agg(sum("addebiti_in_sosp").alias("addebiti_in_sosp"))

    // 357

    // JOIN cicli_ucol_udct_urda  BY (cd_isti_coll, ndg_coll, dt_riferimento_ucol ) LEFT
    //     ,tlbucoc               BY (cd_istituto, ndg, dt_riferimento );

    val fileOutDistJoinCondition = ((cicliUcolUdctUrda("cd_isti_coll") === tlbucoc("cd_istituto"))
      && (cicliUcolUdctUrda("ndg_coll") === tlbucoc("ndg"))
      && (cicliUcolUdctUrda("dt_riferimento_ucol") === tlbucoc("dt_riferimento")))

    val fileOutDist = cicliUcolUdctUrda.join(tlbucoc, fileOutDistJoinCondition, "left_outer")
      .select(cicliUcolUdctUrda("cd_isti"), cicliUcolUdctUrda("ndg_principale"), cicliUcolUdctUrda("dt_inizio_ciclo"), cicliUcolUdctUrda("dt_fine_ciclo"),
        cicliUcolUdctUrda("datainiziopd"), cicliUcolUdctUrda("datainizioristrutt"), cicliUcolUdctUrda("datainizioinc"), cicliUcolUdctUrda("datainiziosoff"),
        cicliUcolUdctUrda("progr"), cicliUcolUdctUrda("cd_isti_coll"), cicliUcolUdctUrda("ndg_coll"), cicliUcolUdctUrda("dt_riferimento_udct"),
        cicliUcolUdctUrda("status_ndg"), cicliUcolUdctUrda("posiz_soff_inc"), cicliUcolUdctUrda("cd_rap_ristr"), cicliUcolUdctUrda("tp_cli_scad_scf"),
        cicliUcolUdctUrda("dt_riferimento_urda"), cicliUcolUdctUrda("util_cassa"), cicliUcolUdctUrda("imp_accordato"),
        tlbucoc("addebiti_in_sosp").alias("tot_add_sosp"), cicliUcolUdctUrda("cd_collegamento")).distinct()

    writeDataFrameAsCsvToPath(fileOutDist, cicliLavStep2OutDirPath)
  }

}
