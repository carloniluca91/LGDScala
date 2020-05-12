package it.carloni.luca.lgd.spark.step

import it.carloni.luca.lgd.spark.utils.ScalaUtils
import it.carloni.luca.lgd.schema.MovimentiSchema
import it.carloni.luca.lgd.scopt.config.DtOsservazioneConfig
import it.carloni.luca.lgd.spark.common.{AbstractSparkStep, SparkEnums}
import org.apache.spark.sql.functions.col
import org.apache.log4j.Logger
import org.apache.spark.sql.Column

import scala.collection.mutable

class Movimenti extends AbstractSparkStep[DtOsservazioneConfig] {

  private val logger = Logger.getLogger(getClass)

  // STEP PATHS
  private val tlbmovcontaCsvPath = getPropertyValue("movimenti.tlbmovconta.csv")
  private val movimentiOuputPath = getPropertyValue("movimenti.mov.out.dist")

  // STEP SCHEMAS
  private val tlbmovcontaPigSchema = MovimentiSchema.tlbmovcontaPigSchema

  // STEP PARAMETERS
  private val dataOsservazionePattern = SparkEnums.DateFormats.DataOsservazioneFormat.toString
  private val Y4M2D2Pattern = SparkEnums.DateFormats.Y4M2D2Format.toString

  override def run(dtOsservazioneConfig: DtOsservazioneConfig): Unit = {

    val dataOsservazione: String = dtOsservazioneConfig.dataOsservazione

    logger.info(dtOsservazioneConfig)

    logger.info(s"movimenti.tlbmovconta.csv: $tlbmovcontaCsvPath")
    logger.info(s"movimenti.mov.out.dist: $movimentiOuputPath")

    val renamingMap: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap(

      "mo_istituto" -> "istituto",
      "mo_ndg" -> "ndg",
      "mo_dt_riferimento" -> "datariferimento",
      "mo_sportello" -> "sportello",
      "mo_conto_esteso" -> "conto",
      "mo_num_soff" -> "numerosofferenza",
      "mo_cat_rapp_soff" -> "catrappsoffer",
      "mo_fil_rapp_soff" -> "filrappsoffer",
      "mo_num_rapp_soff" -> "numrappsoffer",
      "mo_id_movimento" -> "idmovimento",
      "mo_categoria" -> "categoria",
      "mo_causale" -> "casuale",
      "mo_dt_contabile" -> "dtcontab",
      "mo_dt_valuta" -> "dtvaluta",
      "mo_imp_movimento" -> "importo",
      "mo_flag_extracont" -> "flagextracontab",
      "mo_flag_storno" -> "flagstorno"
    )

    val renamedColSeq: Seq[Column] = (for((key, value) <- renamingMap) yield col(key).as(value)).toSeq

    val dataOsservazioneFormatted = ScalaUtils.changeDateFormat(dataOsservazione, dataOsservazionePattern, Y4M2D2Pattern)
    val movout = readCsvFromPathUsingSchema(tlbmovcontaCsvPath, tlbmovcontaPigSchema)
      .filter(col("mo_dt_contabile") <= dataOsservazioneFormatted)
      .select(renamedColSeq: _*)
      .distinct()

    writeDataFrameAsCsvToPath(movout, movimentiOuputPath)
  }
}
