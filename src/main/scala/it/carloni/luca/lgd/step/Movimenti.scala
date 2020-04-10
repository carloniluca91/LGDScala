package it.carloni.luca.lgd.step

import it.carloni.luca.lgd.commons.LGDCommons
import it.carloni.luca.lgd.spark.AbstractSparkStep
import it.carloni.luca.lgd.spark.utils.ScalaUtils
import it.carloni.luca.lgd.schema.MovimentiSchema
import org.apache.spark.sql.functions.col
import org.apache.log4j.Logger

class Movimenti(private val dataOsservazione: String)
  extends AbstractSparkStep {

  private val logger = Logger.getLogger(getClass)

  // STEP PATHS
  private val tlbmovcontaCsvPath = getPropertyValue("movimenti.tlbmovconta.csv")
  private val movimentiOuputPath = getPropertyValue("movimenti.mov.out.dist")

  // STEP SCHEMAS
  private val tlbmovcontaPigSchema = MovimentiSchema.tlbmovcontaPigSchema

  // STEP PARAMETERS
  private val dataOsservazionePattern = LGDCommons.DatePatterns.DataOsservazionePattern
  private val Y4M2D2Pattern = LGDCommons.DatePatterns.Y4M2D2Pattern

  override def run(): Unit = {

    logger.debug(s"tlbmovcontaCsvPath: $tlbmovcontaCsvPath")
    logger.debug(s"movimentiOuputPath: $movimentiOuputPath")
    logger.debug(s"dataOsservazione: $dataOsservazione")

    val dataOsservazioneFormatted = ScalaUtils.changeLocalDateFormat(dataOsservazione, dataOsservazionePattern, Y4M2D2Pattern)
    val movout = readCsvFromPathUsingSchema(tlbmovcontaCsvPath, tlbmovcontaPigSchema)
      .filter(col("mo_dt_contabile") <= dataOsservazioneFormatted)
      .distinct()

    writeDataFrameAsCsvToPath(movout, movimentiOuputPath)
  }
}
