package it.carloni.luca.lgd.step

import it.carloni.luca.lgd.commons.LGDCommons
import it.carloni.luca.lgd.spark.AbstractSparkStep
import it.carloni.luca.lgd.spark.utils.ScalaUtils.changeLocalDateFormat
import it.carloni.luca.lgd.spark.utils.SparkUtils.{addDuration, leastDate, subtractDuration}
import it.carloni.luca.lgd.schema.FanagMonthlySchema
import org.apache.spark.sql.functions.{col, substring, when}
import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger

class FanagMonthly(private val dataA: String, private val numeroMesi1: Int, private val numeroMesi2: Int)
  extends AbstractSparkStep {

  private val logger = Logger.getLogger(getClass)

  // STEP PATHS
  private val cicliNdgCsvPath = getPropertyValue("fanag.monthly.cicli.ndg.path.csv")
  private val tlbuactCsvPath = getPropertyValue("fanag.monthly.tlbuact.csv")
  private val tlbudtcCsvPath = getPropertyValue("fanag.monthly.tlbudtc.path.csv")
  private val fanagOutputPath = getPropertyValue("fanag.monthly.fanag.out")

  // STEP SCHEMAS
  private val cicliNdgPigSchema = FanagMonthlySchema.cicliNdgPigSchema
  private val tlbuactLoadPigSchema = FanagMonthlySchema.tlbuactLoadPigSchema
  private val tlbudtcPigSchema = FanagMonthlySchema.tlbudtcPigSchema

  override def run(): Unit = {

    logger.debug(s"cicliNdgCsvPath: $cicliNdgCsvPath")
    logger.debug(s"tlbuactCsvPath: $tlbuactCsvPath")
    logger.debug(s"tlbudtcCsvPath: $tlbudtcCsvPath")
    logger.debug(s"fanagOutputPath: $fanagOutputPath")
    logger.debug(s"dataA: $dataA")
    logger.debug(s"numeroMesi1: $numeroMesi1")
    logger.debug(s"numeroMesi2: $numeroMesi2")

    val cicliNdg = readCsvFromPathUsingSchema(cicliNdgCsvPath, cicliNdgPigSchema)

    // cicli_ndg_princ = FILTER cicli_ndg BY cd_collegamento IS NULL;
    // cicli_ndg_coll = FILTER cicli_ndg BY cd_collegamento IS NOT NULL;

    val cicliNdgPrinc = cicliNdg.filter(col("cd_collegamento").isNull)
    val cicliNdgColl = cicliNdg.filter(col("cd_collegamento").isNotNull)

    val tlbuact = readCsvFromPathUsingSchema(tlbuactCsvPath, tlbuactLoadPigSchema)
      .selectExpr("dt_riferimento", "cd_istituto", "ndg", "tp_ndg", "intestazione", "cd_fiscale",
        "partita_iva", "sae", "rae", "ciae", "provincia", "sportello", "ndg_caponucleo")

    val tlbcidefTlbuact: DataFrame = Seq(cicliNdgPrinc, cicliNdgColl).map((cicliNdgDf: DataFrame) => {

      // JOIN  tlbuact BY (cd_istituto, ndg), cicli_ndg_princ BY (codicebanca_collegato, ndg_collegato);
      // JOIN  tlbuact BY (cd_istituto, ndg), cicli_ndg_coll BY (codicebanca_collegato, ndg_collegato);

      val joinConditionCol = (tlbuact("cd_istituto") === cicliNdgDf("codicebanca_collegato")) &&
        (tlbuact("ndg") === cicliNdgDf("ndg_collegato"))

      // FILTER tlbcidef_coll_tlbuact_join
      // BY ToDate((chararray)dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')
      // AND SUBSTRING( (chararray)dt_riferimento,0,6 ) <=
      // SUBSTRING(ToString(AddDuration( ToDate( (chararray)LeastDate( (int)ToString(
      // SubtractDuration(ToDate((chararray)datafinedef,'yyyyMMdd' ),'P1M'),'yyyyMMdd'), $data_a ),'yyyyMMdd' ),'$numero_mesi_2' ),'yyyyMMdd'),0,6 )

      // PARSE $data_a IN ORDER TO FIT WITH LEAST_DATE UDF
      val Y4M2D2Format = "yyyyMMdd"
      val dataAToY4M2D2 = changeLocalDateFormat(dataA, LGDCommons.DatePatterns.DataAPattern, Y4M2D2Format)
      val dataFineDefSubtractDurationCol = subtractDuration(cicliNdgDf("datafinedef"), Y4M2D2Format, 1)
      val leastDateSubtractDurationCol = leastDate(dataFineDefSubtractDurationCol, dataAToY4M2D2, Y4M2D2Format)
      val addDurationLeastDateCol = addDuration(leastDateSubtractDurationCol, Y4M2D2Format, numeroMesi2)
      val substringAddDurationCol = substring(addDurationLeastDateCol, 0, 6)
      val dtRiferimentoDataFineDefFilterCol = substring(tlbuact("dt_riferimento"), 0, 6) <= substringAddDurationCol
      val dtRiferimentoDataInizioDefFilterCol = tlbuact("dt_riferimento") >= subtractDuration(cicliNdgDf("datainiziodef"), Y4M2D2Format, numeroMesi1)
      val filterConditionCol = dtRiferimentoDataInizioDefFilterCol && dtRiferimentoDataFineDefFilterCol

      /*
      		      cicli_ndg_princ::codicebanca_collegato as codicebanca_collegato
				 	 		 ,cicli_ndg_princ::ndg_collegato  as ndg_collegato
							 ,cicli_ndg_princ::datainiziodef  as datainiziodef
							 ,cicli_ndg_princ::datafinedef  as datafinedef
							 ,tlbuact::dt_riferimento as datariferimento
							 ,tlbuact::tp_ndg as naturagiuridica
							 ,tlbuact::intestazione as intestazione
							 ,tlbuact::cd_fiscale as codicefiscale
							 ,tlbuact::partita_iva as partitaiva
							 ,tlbuact::sae as sae
							 ,tlbuact::rae as rae
							 ,tlbuact::ciae as ciae
							 ,cicli_ndg_princ::provincia_segm as  provincia
							 ,tlbuact::provincia as provincia_cod
							 ,tlbuact::sportello as  sportello
							 ,cicli_ndg_princ::segmento as segmento
							 ,cicli_ndg_princ::cd_collegamento as cd_collegamento
							 ,tlbuact::ndg_caponucleo as ndg_caponucleo
							 ,cicli_ndg_princ::codicebanca  as codicebanca
							 ,cicli_ndg_princ::ndgprincipale  as ndgprincipale
       */

      tlbuact.join(cicliNdgDf, joinConditionCol)
        .filter(filterConditionCol)
        .select(cicliNdgDf("codicebanca_collegato"), cicliNdgDf("ndg_collegato"), cicliNdgDf("datainiziodef"), cicliNdgDf("datafinedef"),
          tlbuact("dt_riferimento").as("datariferimento"), tlbuact("tp_ndg").as("naturagiuridica"), tlbuact("intestazione"),
          tlbuact("cd_fiscale").as("codicefiscale"), tlbuact("partita_iva").as("partitaiva"), tlbuact("sae"), tlbuact("rae"),
          tlbuact("ciae"), cicliNdgDf("provincia_segm").as("provincia"), tlbuact("provincia").as("provincia_cod"), tlbuact("sportello"),
          cicliNdgDf("segmento"), cicliNdgDf("cd_collegamento"), tlbuact("ndg_caponucleo"), cicliNdgDf("codicebanca"), cicliNdgDf("ndgprincipale"))
    })
      .reduce(_ union _)
      .distinct

    val tlbudtc = readCsvFromPathUsingSchema(tlbudtcCsvPath, tlbudtcPigSchema)

    // JOIN  tlbudtc BY (cd_istituto, ndg, dt_riferimento), tlbcidef_tlbuact BY (codicebanca_collegato,ndg_collegato, datariferimento) ;

    val tlbudctTlbcidefTlbuactJoinConditionCol = (tlbudtc("cd_istituto") === tlbcidefTlbuact("codicebanca_collegato")) &&
      (tlbudtc("ndg") === tlbcidefTlbuact("ndg_collegato")) &&
      (tlbudtc("dt_riferimento") === tlbcidefTlbuact("datariferimento"))

    /*
           tlbcidef_tlbuact::codicebanca_collegato as codicebanca
					,tlbcidef_tlbuact::ndg_collegato  as ndg
					,tlbcidef_tlbuact::datariferimento as datariferimento
					,tlbudtc::totale_accordato as totaccordato
					,tlbudtc::totale_utilizzi as totutilizzo
					,tlbudtc::tot_acco_mortgage as totaccomortgage
					,tlbudtc::tot_util_mortgage as totutilmortgage
					,tlbudtc::totale_saldi_0063 as totsaldi0063
					,tlbudtc::totale_saldi_0260 as totsaldi0260
					,tlbcidef_tlbuact::naturagiuridica as naturagiuridica
					,tlbcidef_tlbuact::intestazione as intestazione
					,tlbcidef_tlbuact::codicefiscale as codicefiscale
					,tlbcidef_tlbuact::partitaiva as partitaiva
					,tlbcidef_tlbuact::sae as sae
					,tlbcidef_tlbuact::rae as rae
					,tlbcidef_tlbuact::ciae as ciae
					,tlbcidef_tlbuact::provincia as  provincia
					,tlbcidef_tlbuact::provincia_cod as provincia_cod
					,tlbcidef_tlbuact::sportello as  sportello
					,tlbudtc::posiz_soff_inc as attrn011
					,tlbudtc::status_ndg as attrn175
					,tlbudtc::tp_cli_scad_scf as attrn186
					,tlbudtc::cd_rap_ristr as cdrapristr
					,tlbcidef_tlbuact::segmento as segmento
					,tlbcidef_tlbuact::cd_collegamento as cd_collegamento
					,tlbcidef_tlbuact::ndg_caponucleo as ndg_caponucleo
					,(tlbudtc::tp_ristrutt != '0' ? 'S' : 'N') as flag_ristrutt
					,tlbcidef_tlbuact::codicebanca    as codicebanca_princ
					,tlbcidef_tlbuact::ndgprincipale  as ndgprincipale
					,tlbcidef_tlbuact::datainiziodef  as datainiziodef
     */

    val flagRistruttCol = when(tlbudtc("tp_ristrutt") =!= "0", "S").otherwise("N").as("flag_ristrutt")
    val fanagOut = tlbudtc.join(tlbcidefTlbuact, tlbudctTlbcidefTlbuactJoinConditionCol)
      .select(tlbcidefTlbuact("codicebanca_collegato").as("codicebanca"), tlbcidefTlbuact("ndg_collegato").as("ndg"),
        tlbcidefTlbuact("datariferimento"), tlbudtc("totale_accordato").as("totaccordato"),
        tlbudtc("totale_utilizzi").as("totutilizzo"), tlbudtc("tot_acco_mortgage").as("totaccomortgage"),
        tlbudtc("tot_util_mortgage").as("totutilmortgage"), tlbudtc("totale_saldi_0063").as("totsaldi0063"),
        tlbudtc("totale_saldi_0260").as("totsaldi0260"), tlbcidefTlbuact("naturagiuridica"), tlbcidefTlbuact("intestazione"),
        tlbcidefTlbuact("codicefiscale"), tlbcidefTlbuact("partitaiva"), tlbcidefTlbuact("sae"), tlbcidefTlbuact("rae"),
        tlbcidefTlbuact("ciae"), tlbcidefTlbuact("provincia"), tlbcidefTlbuact("provincia_cod"), tlbcidefTlbuact("sportello"),
        tlbudtc("posiz_soff_inc").as("attrn011"), tlbudtc("status_ndg").as("attrn175"),
        tlbudtc("tp_cli_scad_scf").as("attrn186"), tlbudtc("cd_rap_ristr").as("cdrapristr"), tlbcidefTlbuact("segmento"),
        tlbcidefTlbuact("cd_collegamento"), tlbcidefTlbuact("ndg_caponucleo"), flagRistruttCol,
        tlbcidefTlbuact("codicebanca").as("codicebanca_princ"), tlbcidefTlbuact("ndgprincipale"), tlbcidefTlbuact("datainiziodef"))

    writeDataFrameAsCsvToPath(fanagOut, fanagOutputPath)
  }
}
