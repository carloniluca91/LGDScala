package it.carloni.luca.lgd.step

import it.carloni.luca.lgd.common.AbstractStep
import it.carloni.luca.lgd.common.utils.LGDCommons
import it.carloni.luca.lgd.common.utils.ScalaUtils.changeLocalDateFormat
import it.carloni.luca.lgd.common.utils.SparkUtils.toIntType
import it.carloni.luca.lgd.schema.CicliPreviewSchema
import it.carloni.luca.lgd.scopt.DataAUfficioParser.Config
import org.apache.spark.sql.functions.{coalesce, col, lit, when}
import org.apache.log4j.Logger
import org.apache.spark.sql.Column

class CicliPreview(dataAUfficioConfig: Config) extends AbstractStep {

  private val logger = Logger.getLogger(getClass)

  // STEP PATHS
  private val fposiOutdirCsvPath = getPropertyValue("cicli.preview.fposi.outdir.csv")
  private val fposiGen2OutputPath = getPropertyValue("cicli.preview.fposi.gen2")
  private val fposiSintGen2OutputPath = getPropertyValue("cicli.preview.fposi.sint.gen2")

  // STEP SCHEMAS
  private val fposiLoadPigSchema = CicliPreviewSchema.fposiLoadPigSchema

  // STEP PARAMETERS
  private val dataA = dataAUfficioConfig.dataA
  private val ufficio = dataAUfficioConfig.ufficio

  override def run(): Unit = {

    logger.debug(s"fposiOutdirCsvPath: $fposiOutdirCsvPath")
    logger.debug(s"fposiGen2OutputPath: $fposiGen2OutputPath")
    logger.debug(s"fposiSintGen2OutputPath: $fposiSintGen2OutputPath")
    logger.debug(s"dataA: $dataA")
    logger.debug(s"ufficio: $ufficio")

    val fposiLoadStructType = fromPigSchemaToStructType(fposiLoadPigSchema)
    val fposiLoad = readCsvFromPathUsingSchema(fposiOutdirCsvPath, fposiLoadStructType)

    // ,ToString(ToDate('$data_a','yyyyMMdd'),'yyyy-MM-dd') as datarif
    val dataRifCol = lit(changeLocalDateFormat(dataA, LGDCommons.DatePatterns.DataAPattern, "yyyy-MM-dd")).as("datarif")

    // (naturagiuridica_segm != 'CO' AND segmento in ('01','02','03','21')?'IM': (segmento == '10'?'PR':'AL')) as segmento_calc
    val segmentoCalcCol = when((col("naturagiuridica_segm") =!= "CO") && col("segmento").isin("01", "02", "03", "21"),
    "IM").otherwise(when(col("segmento") === "10", "PR").otherwise("AL")).as("segmento_calc")

    // ( datasofferenza is null?'N':'S') as ciclo_soff
    val cicloSoffCol = when(col("datasofferenza").isNull, "N").otherwise("N").as("ciclo_soff")

    // ( datainiziopd is not null
    // and (datainiziopd<(datasofferenza is null?'99999999':datasofferenza)
    // and datainiziopd<(datainizioinc is null?'99999999':datainizioinc)
    // and datainiziopd<(datainizioristrutt is null?'99999999':datainizioristrutt))? 'PASTDUE':
    val dataInizioPdWhenCol = when(col("datainiziopd").isNotNull
      && (col("datainiziopd") < getOrElse(col("datasofferenza")))
      && (col("datainiziopd") < getOrElse(col("datainizioinc")))
      && (col("datainiziopd") < getOrElse(col("datainizioristrutt"))), "PASTDUE")

    // ( datainizioinc is not null
    // and (datainizioinc<(datainiziopd is null?'99999999':datainiziopd)
    // and datainizioinc<(datasofferenza is null?'99999999':datasofferenza)
    // and datainizioinc<(datainizioristrutt is null?'99999999':datainizioristrutt))? 'INCA':
    val dataInizioIncWhenCol = when(col("datainizioinc").isNotNull
      && (col("datainizioinc") < getOrElse(col("datainiziopd")))
      && (col("datainizioinc") < getOrElse(col("datasofferenza")))
      && (col("datainizioinc") < getOrElse(col("datainizioristrutt"))), "INCA")

    // ( datainizioristrutt is not null
    // and (datainizioristrutt<(datainiziopd is null?'99999999':datainiziopd)
    // and datainizioristrutt<(datainizioinc is null?'99999999':datainizioinc)
    // and datainizioristrutt<(datasofferenza is null?'99999999':datasofferenza))? 'RISTR':
    val dataInizioRistruttWhenCol = when(col("datainizioristrutt").isNotNull
      && (col("datainizioristrutt") < getOrElse(col("datainiziopd")))
      && (col("datainizioristrutt") < getOrElse(col("datainizioinc")))
      && (col("datainizioristrutt") < getOrElse(col("datasofferenza"))), "RISTR")

    // ( datasofferenza is not null
    // and (datasofferenza<(datainiziopd is null?'99999999':datainiziopd)
    // and datasofferenza<(datainizioinc is null?'99999999':datainizioinc)
    // and datasofferenza<(datainizioristrutt is null?'99999999':datainizioristrutt))? 'SOFF': 'PASTDUE'
    val dataSofferenzaWhenCol = when(col("datasofferenza").isNotNull
      && (col("datasofferenza") < getOrElse(col("datainiziopd")))
      && (col("datasofferenza") < getOrElse(col("datainizioinc")))
      && (col("datasofferenza") < getOrElse(col("datainizioristrutt"))), "SOFF").otherwise("PASTDUE")

    // ( (datainiziopd is null and datainizioinc is null and datainizioristrutt is null and datasofferenza is null)?'PASTDUE':
    val dataInizioPdIncRistruttSofferenzaCoalesceCol = when(coalesce(col("datainiziopd"), col("datainizioinc"),
      col("datainizioristrutt"), col("datasofferenza")).isNull, "PASTDUE")

    val statoAnagraficoCol = dataInizioPdIncRistruttSofferenzaCoalesceCol
      .otherwise(dataInizioPdWhenCol.otherwise(dataInizioIncWhenCol.otherwise(dataInizioRistruttWhenCol.otherwise(dataSofferenzaWhenCol))))
      .as("stato_anagrafico")

    // ,( (int)datafinedef > $data_a ? 'A' : 'C' ) as flag_aperto
    val flagApertoCol = when(toIntType(col("datafinedef")) > dataA.toInt, "A").otherwise("C").as("flag_aperto")

    val fposiBase = fposiLoad
      .select(lit(ufficio).as("ufficio"), col("codicebanca"), dataRifCol, col("ndgprincipale"),
        col("datainiziodef"), col("datafinedef"), col("datainiziopd"), col("datainizioinc"),
        col("datainizioristrutt"), col("datasofferenza"), col("totaccordatodatdef"),
        col("totutilizzdatdef"), segmentoCalcCol, cicloSoffCol, statoAnagraficoCol, flagApertoCol)

    // TODO: prosegui da 83
  }

  private def getOrElse(column: Column): Column = when(column.isNotNull, column).otherwise("99999999")
}
