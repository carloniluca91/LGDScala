package it.carloni.luca.lgd.scopt.option

import it.carloni.luca.lgd.spark.common.SparkEnums.DateFormats


object OptionNaming {

  val StepNameShortOption: Char = 's'
  val StepNameLongOption = "step-name"
  val StepNameOptionDescription = "job Spark da eseguire"

  val DataDaLongOption = "data-da"
  val DataDaOptionDescription = s"parametro $$data_da (formato ${DateFormats.DataDaFormat.toString})"
  val DataDaFailedValidationMessage = s"parametro $$data_da non conforme (atteso ${DateFormats.DataDaFormat.toString}, ricevuto "

  val DataALongOption = "data-a"
  val DataAOptionDescription = s"parametro $$data_a (formato ${DateFormats.DataAFormat.toString})"
  val DataAFailedValidationMessage = s"parametro $$data_a non conforme (atteso ${DateFormats.DataAFormat.toString}, ricevuto "

  val DataOsservazioneLongOption = "data-osservazione"
  val DataOsservazioneOptionDescription = s"parametro $$data_osservazione (formato ${DateFormats.DataOsservazioneFormat.toString})"
  val DataOsservazioneFailedValidationMessage = s"parametro $$data_a non conforme (atteso ${DateFormats.DataOsservazioneFormat.toString}, ricevuto "

  val UfficioLongOption = "ufficio"
  val UfficioOptionDescription = "parametro $ufficio"

  val NumeroMesi1LongOption = "numero-mesi-1"
  val NumeroMesi1Description = "parametro $numero_mesi_1"

  val NumeroMesi2LongOption = "numero-mesi-2"
  val NumeroMesi2Description = "parametro $numero_mesi_2"

  val PeriodoLongOption = "periodo"
  val PeriodoOptionDescription = s"parametro $$periodo (formato ${DateFormats.PeriodoFormat.toString}"

}
