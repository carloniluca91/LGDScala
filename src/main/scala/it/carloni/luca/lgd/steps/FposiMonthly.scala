package it.carloni.luca.lgd.steps

import it.carloni.luca.lgd.common.AbstractStep
import it.carloni.luca.lgd.common.utils.{SparkUtils, LGDCommons}
import it.carloni.luca.lgd.options.OptionNaming
import it.carloni.luca.lgd.schemas.FposiMonthlySchema
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{lit, max, min, sum, when}

import scala.collection.mutable

class FposiMonthly(stepParamsMap: mutable.Map[String, String])
  extends AbstractStep {

  private val logger = Logger.getLogger(getClass)
  private val tlbcidef1Schema = FposiMonthlySchema.tlbcidef1PigSchema
  private val ciclilavListadefSchema = FposiMonthlySchema.ciclilavListadefPigSchema
  private val tlbungrSchema = FposiMonthlySchema.tlbungrPigSchema
  private val tlbuactLoadSchema = FposiMonthlySchema.tlbuactLoadPigSchema

  override def run() {

    val periodo = stepParamsMap(OptionNaming.PeriodoLongOption)
    val dataA = stepParamsMap(OptionNaming.DataALongOption)
    val periodoPattern = LGDCommons.DatePatterns.PeriodoPattern
    val tlbcidef1Path = getValue("fposi_monthly.input.table.tlbcidef1")
    val ciclilavListadefPath = getValue("fposi_monthly.input.table.ciclilav_listadef")
    val tlbungrPath = getValue("fposi_monthly.input.table.tlbungr")
    val tlbuactLoadPath = getValue("fposi_monthly.input.table.tlbuact_load")
    val fposiMonthlyOutputDirPath = getValue("fposi_monthly.data.output.dir")

    logger.debug(f"periodo: $periodo")
    logger.debug(f"dataA $dataA")
    logger.debug(f"periodoPattern: $periodoPattern")
    logger.debug(f"tlbcidef1Path: $tlbcidef1Path")
    logger.debug(f"ciclilavListadefPath: $ciclilavListadefPath")
    logger.debug(f"tlbungrPath: $tlbungrPath")
    logger.debug(f"tlbuactLoadPath: $tlbuactLoadPath")
    logger.debug(f"fposiMonthlyOutputDirPath: $fposiMonthlyOutputDirPath")

    // 24

    val tlbcidef1StructTypeSchema = fromPigSchemaToStructType(tlbcidef1Schema)
    val tlbcidef1 = readCsvFromPathUsingSchema(tlbcidef1Path, tlbcidef1StructTypeSchema)

    val ciclilavListaDefStructTyeSchema = fromPigSchemaToStructType(ciclilavListadefSchema)
    val ciclilavListaDef = readCsvFromPathUsingSchema(ciclilavListadefPath, ciclilavListaDefStructTyeSchema)

    // 62

    // 71

    /*
    tlbcidef_filter1 = FILTER tlbcidef1 BY SUBSTRING((chararray)datainiziodef,0,6) >= SUBSTRING('$periodo',0,6)
    AND SUBSTRING((chararray)datainiziodef,0,6) <= ( SUBSTRING('$periodo',7,13) is null ? SUBSTRING('$periodo',0,6) : SUBSTRING('$periodo',7,13) )
     */

    val substringPeriodo07 = periodo.substring(0, 7)
    val substringPeriodo714 = if ((periodo.length < 14) && (periodo.length > 7)) substringPeriodo07  else periodo.substring(7, 14)

    val tlbcidefFilterCol = (SparkUtils.isDateGeqOtherDate(tlbcidef1("datainiziodef"), periodoPattern, lit(substringPeriodo07), periodoPattern)
      && SparkUtils.isDateLeqOtherDate(tlbcidef1("datainiziodef"), periodoPattern, lit(substringPeriodo714), periodoPattern))

    val tlbcidefFilter1 = tlbcidef1.filter(tlbcidefFilterCol)

    // 73

    // 75

    /*
    cicli_lista_join = JOIN  tlbcidef_filter1   BY ( codicebanca, ndgprincipale, datainiziodef, codicebanca_collegato )
                            ,ciclilav_listadef  BY ( cd_isti, ndg_principale, dt_inizio_ciclo, cd_isti_coll );
     */

    val tlbcidefFilterJoinConditionCol = ((tlbcidefFilter1("codicebanca") === ciclilavListaDef("cd_isti"))
      && (tlbcidefFilter1("ndgprincipale") === ciclilavListaDef("ndg_principale"))
      && (tlbcidefFilter1("datainiziodef") === ciclilavListaDef("dt_inizio_ciclo"))
      && (tlbcidefFilter1("codicebanca_collegato") === ciclilavListaDef("cd_isti_coll")))

    val tlbcidefFilter = tlbcidefFilter1.join(ciclilavListaDef, tlbcidefFilterJoinConditionCol).select(tlbcidefFilter1("*"))

    // 106

    // 108

    /*
    cicli_minudct_lista = FOREACH cicli_group GENERATE
                         group.codicebanca_collegato       as codicebanca_collegato
			                  ,group.ndg_collegato               as ndg_collegato
					              ,group.datainiziodef               as datainiziodef
			   		            ,MIN(tlbcidef_filter.dt_rif_udct)  as dt_rif_udct;
     */

    val cicliMinudctLista = tlbcidefFilter.groupBy("codicebanca_collegato", "ndg_collegato", "datainiziodef")
      .agg(min(tlbcidefFilter("dt_rif_udct")).alias("dt_rif_udct"))

    // 120

    // 122

    /*
    cicli_lista_join = JOIN tlbcidef_filter     BY ( codicebanca_collegato, ndg_collegato, datainiziodef, dt_rif_udct )
                           ,cicli_minudct_lista BY ( codicebanca_collegato, ndg_collegato, datainiziodef, dt_rif_udct );
     */

    val tlbcidefSeqJoinColumns = Seq("codicebanca_collegato", "ndg_collegato", "datainiziodef", "dt_rif_udct")
    val tlbcidef = tlbcidefFilter.join(cicliMinudctLista, tlbcidefSeqJoinColumns).select(tlbcidefFilter("*"))

    // 153

    // 158

    val tlbungrStructTypeSchema = fromPigSchemaToStructType(tlbungrSchema)
    val tlbungr = readCsvFromPathUsingSchema(tlbungrPath, tlbungrStructTypeSchema)

    // tlbcidef_tlbungr_join = JOIN tlbcidef BY (codicebanca, datainiziodef, ndgprincipale) LEFT,
    //                              tlbungr BY (cd_istituto, dt_riferimento, ndg);

    val tlbcidefTlbungrJoinConditionCol = ((tlbcidef("codicebanca") === tlbungr("cd_istituto"))
      && (tlbcidef("datainiziodef") === tlbungr("dt_riferimento"))
      && (tlbcidef("ndgprincipale") === tlbungr("ndg")))

    val tlbcidefTlbungr = tlbcidef.join(tlbungr, tlbcidefTlbungrJoinConditionCol, "left")
      .select(tlbcidef("codicebanca"), tlbcidef("ndgprincipale"), tlbcidef("datainiziodef"), tlbcidef("datafinedef"), tlbcidef("datainiziopd"),
        tlbcidef("datainizioristrutt"), tlbcidef("datainizioinc"), tlbcidef("datainiziosoff"), tlbcidef("c_key"), tlbcidef("tipo_segmne"),
        tlbcidef("sae_segm"), tlbcidef("rae_segm"), tlbcidef("segmento"), tlbcidef("tp_ndg"), tlbcidef("provincia_segm"), tlbcidef("databilseg"),
        tlbcidef("strbilseg"), tlbcidef("attivobilseg"), tlbcidef("fatturbilseg"), tlbcidef("ndg_collegato"), tlbcidef("codicebanca_collegato"),
        tlbcidef("cd_fiscale"), tlbcidef("cd_fiscale").alias("partita_iva"), tlbungr("ndg_gruppo"), tlbcidef("util_cassa").alias("totutilizzdatdef"),
        tlbcidef("imp_accordato").alias("totaccordatodatdef"))

    // 200

    // 203

    val tlbcidefTlbungrTlbposiSum = tlbcidefTlbungr.groupBy("codicebanca", "ndgprincipale" , "datainiziodef")
      .agg(max("datafinedef").alias("datafinedef"), max("datainiziopd").alias("datainiziopd"), max("datainizioristrutt").alias("datainizioristrutt"),
        max("datainizioinc").alias("datainizioinc"), max("datainiziosoff").alias("datainiziosoff"), max("c_key").alias("c_key"),
        max("tipo_segmne").alias("tipo_segmne"), max("sae_segm").alias("sae_segm"), max("rae_segm").alias("rae_segm"),
        max("segmento").alias("segmento"), max("tp_ndg").alias("tp_ndg"), max("provincia_segm").alias("provincia_segm"),
        max("databilseg").alias("databilseg"), max("strbilseg").alias("strbilseg"), max("attivobilseg").alias("attivobilseg"),
        max("fatturbilseg").alias("fatturbilseg"), max("ndg_gruppo").alias("ndg_gruppo"), max("cd_fiscale").alias("cd_fiscale"),
        max("partita_iva").alias("partita_iva"), sum("totaccordatodatdef").alias("totaccordatodatdef"), sum("totutilizzdatdef").alias("totutilizzdatdef"))

    // 230

    // 233

    val tlbuactLoadStructTypeSchema = fromPigSchemaToStructType(tlbuactLoadSchema)
    val tlbuact = readCsvFromPathUsingSchema(tlbuactLoadPath, tlbuactLoadStructTypeSchema)
      .selectExpr("dt_riferimento", "cd_istituto", "ndg", "intestazione", "ciae", "n160_cod_ateco")

    // 281

    // tlbcidef_tlbungr_tlbposi_tlbuact_join =
    // JOIN tlbcidef_tlbungr_tlbposi_sum BY (datainiziodef, codicebanca, ndgprincipale) LEFT,
    //                           tlbuact BY (dt_riferimento, cd_istituto, ndg);

    val fposiOutDistJoinCondition = ((tlbcidefTlbungrTlbposiSum("datainiziodef") === tlbuact("dt_riferimento"))
      && (tlbcidefTlbungrTlbposiSum("codicebanca") === tlbuact("cd_istituto"))
      && (tlbcidefTlbungrTlbposiSum("ndgprincipale") === tlbuact("ndg")))

    /*
        ,( tlbcidef_tlbungr_tlbposi_sum::datafinedef > '$data_a'?'99991231':tlbcidef_tlbungr_tlbposi_sum::datafinedef ) as datafinedef
				,( tlbcidef_tlbungr_tlbposi_sum::datainiziopd > '$data_a'?NULL:tlbcidef_tlbungr_tlbposi_sum::datainiziopd ) as datainiziopd
				,( tlbcidef_tlbungr_tlbposi_sum::datainizioinc > '$data_a'?NULL:tlbcidef_tlbungr_tlbposi_sum::datainizioinc ) as datainizioinc
				,( tlbcidef_tlbungr_tlbposi_sum::datainizioristrutt > '$data_a'?NULL:tlbcidef_tlbungr_tlbposi_sum::datainizioristrutt ) as datainizioristrutt
				,( tlbcidef_tlbungr_tlbposi_sum::datainiziosoff > '$data_a'?NULL:tlbcidef_tlbungr_tlbposi_sum::datainiziosoff ) as datainiziosoff
     */

    val dataFineDefCol = when(tlbcidefTlbungrTlbposiSum("datafinedef").>(lit(dataA)), "99991231").otherwise(tlbcidefTlbungrTlbposiSum("datafinedef"))
    val dataInizioCol = when(tlbcidefTlbungrTlbposiSum("datainiziopd").>(lit(dataA)), null).otherwise(tlbcidefTlbungrTlbposiSum("datainiziopd"))
    val dataInizioIncCol = when(tlbcidefTlbungrTlbposiSum("datainizioinc").>(lit(dataA)), null).otherwise(tlbcidefTlbungrTlbposiSum("datainizioinc"))
    val dataInizioRistruttCol = when(tlbcidefTlbungrTlbposiSum("datainizioristrutt").>(lit(dataA)), null).otherwise(tlbcidefTlbungrTlbposiSum("datainizioristrutt"))
    val dataInizioSoffCol = when(tlbcidefTlbungrTlbposiSum("datainiziosoff").>(lit(dataA)), null).otherwise(tlbcidefTlbungrTlbposiSum("datainiziosoff"))

    val fposiOutDist = tlbcidefTlbungrTlbposiSum.join(tlbuact, fposiOutDistJoinCondition, "left")
      .select(tlbcidefTlbungrTlbposiSum("codicebanca"), tlbcidefTlbungrTlbposiSum("ndgprincipale"), tlbcidefTlbungrTlbposiSum("datainiziodef"),
        dataFineDefCol.alias("datafinedef"), dataInizioCol.alias("datainiziopd"), dataInizioIncCol.alias("datainizioinc"),
        dataInizioRistruttCol.alias("datainizioristrutt"),dataInizioSoffCol.alias("datainiziosoff"),
        tlbcidefTlbungrTlbposiSum("totaccordatodatdef"), tlbcidefTlbungrTlbposiSum("totutilizzdatdef"),
        tlbcidefTlbungrTlbposiSum("segmento"), tlbcidefTlbungrTlbposiSum("tp_ndg").alias("naturagiuridica_segm"))
      .distinct()

    writeDataFrameAsCsvToPath(fposiOutDist, fposiMonthlyOutputDirPath)
  }
}