package it.carloni.luca.lgd.step

import it.carloni.luca.lgd.common.BaseStep
import it.carloni.luca.lgd.common.utils.LGDCommons
import it.carloni.luca.lgd.common.utils.ScalaUtils.changeLocalDateFormat
import it.carloni.luca.lgd.common.utils.SparkUtils.{changeDateFormat, toIntType}
import it.carloni.luca.lgd.schema.CicliPreviewSchema
import it.carloni.luca.lgd.scopt.DataAUfficioParser.DataAUfficioConfig
import org.apache.spark.sql.functions.{coalesce, col, count, lit, substring, sum, when}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.Column
import org.apache.log4j.Logger

class CicliPreview(private val dataAUfficioConfig: DataAUfficioConfig)
  extends BaseStep {

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

    val fposiLoad = readCsvFromPathUsingSchema(fposiOutdirCsvPath, fposiLoadPigSchema)

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

    // GROUP fposi_base BY ( codicebanca, ndgprincipale, datainiziodef );
    val fposiGen2WindowSpec = Window.partitionBy(col("codicebanca"), col("ndgprincipale"), col("datainiziodef"))

    val Y4M2D2format = "yyyyMMdd"
    val Y4_M2_D2Format = "yyyy-MM-dd"

    val fposiGen2 = fposiBase
      .withColumn("datainiziodef", changeDateFormat(col("datainiziodef"), Y4M2D2format, Y4_M2_D2Format))
      .withColumn("datafinedef", changeDateFormat(col("datafinedef"), Y4M2D2format, Y4_M2_D2Format))
      .withColumn("datainiziopd", changeDateFormat(col("datainiziopd"), Y4M2D2format, Y4_M2_D2Format))
      .withColumn("datainizioinc", changeDateFormat(col("datainizioinc"), Y4M2D2format, Y4_M2_D2Format))
      .withColumn("datainizioristrutt", changeDateFormat(col("datainizioristrutt"), Y4M2D2format, Y4_M2_D2Format))
      .withColumn("datasofferenza", changeDateFormat(col("datasofferenza"), Y4M2D2format, Y4_M2_D2Format))
      .withColumn("totaccordatodatdef", sum(col("totaccordatodatdef")).over(fposiGen2WindowSpec).cast(DataTypes.DoubleType))
      .withColumn("totutilizzdatdef", sum(col("totutilizzdatdef")).over(fposiGen2WindowSpec).cast(DataTypes.DoubleType))
      .drop(col("flag_aperto"))

    writeDataFrameAsCsvToPath(fposiGen2, fposiGen2OutputPath)

    /*
       group.ufficio          as ufficio
      ,group.datarif          as datarif
      ,group.flag_aperto      as flag_aperto
      ,group.codicebanca      as codicebanca
      ,group.segmento_calc    as segmento_calc
      ,SUBSTRING(group.$4,0,6) as mese_apertura
      ,SUBSTRING(group.$5,0,6)   as mese_chiusura
      ,group.stato_anagrafico as stato_anagrafico
      ,group.ciclo_soff       as ciclo_soff
      ,COUNT(fposi_base)      as row_count
      ,SUM(fposi_base.totaccordatodatdef) as totaccordatodatdef
      ,SUM(fposi_base.totutilizzdatdef)   as totutilizzdatdef
     */

    val meseAperturaCol = substring(col("datainiziodef"), 0, 6).as("mese_apertura")
    val meseChiusuraCol = substring(col("datafinedef"), 0, 6).as("mese_chiusura")

    val fposiSintGen2 = fposiBase.groupBy(col("ufficio"), col("datarif"), col("flag_aperto"),
      col("codicebanca"), col("segmento_calc"), meseAperturaCol, meseChiusuraCol,
      col("stato_anagrafico"), col("ciclo_soff"))
      .agg(count("*").as("row_count"),
        sum(col("totaccordatodatdef")).cast(DataTypes.DoubleType).as("totaccordatodatdef"),
        sum(col("totutilizzdatdef")).cast(DataTypes.DoubleType).as("totutilizzdatdef"))

    writeDataFrameAsCsvToPath(fposiSintGen2, fposiSintGen2OutputPath)
  }

  private def getOrElse(column: Column): Column = when(column.isNotNull, column).otherwise("99999999")
}
