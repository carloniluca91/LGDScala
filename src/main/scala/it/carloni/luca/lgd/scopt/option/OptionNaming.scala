package it.carloni.luca.lgd.scopt.option

import it.carloni.luca.lgd.common.utils.LGDCommons

object OptionNaming {

  val DataDaLongOption = "data-da"
  val DataDaOptionDescription = s"parametro $$data_da (formato ${LGDCommons.DatePatterns.DataDaPattern})"
  val DataDaFailedValidationMessage = s"parametro $$data_da non conforme (atteso ${LGDCommons.DatePatterns.DataDaPattern}, ricevuto "

  val DataALongOption = "data-a"
  val DataAOptionDescription = s"parametro $$data_a (formato ${LGDCommons.DatePatterns.DataAPattern})"
  val DataAFailedValidationMessage = s"parametro $$data_a non conforme (atteso ${LGDCommons.DatePatterns.DataAPattern}, ricevuto "

  val DataOsservazioneLongOption = "data-osservazione"
  val DataOsservazioneOptionDescription = s"parametro $$data_osservazione (formato ${LGDCommons.DatePatterns.DataOsservazionePattern})"
  val DataOsservazioneFailedValidationMessage = s"parametro $$data_a non conforme (atteso ${LGDCommons.DatePatterns.DataAPattern}, ricevuto "

  val UfficioLongOption = "ufficio"
  val UfficioOptionDescription = "parametro $ufficio"

  val NumeroMesi1LongOption = "numero-mesi-1"
  val NumeroMesi1Description = "parametro $numero_mesi_1"

  val NumeroMesi2LongOption = "numero-mesi-2"
  val NumeroMesi2Description = "parametro $numero_mesi_2"

  val PeriodoLongOption = "periodo"
  val PeriodoOptionDescription = s"parametro $$periodo (formato ${LGDCommons.DatePatterns.PeriodoPattern}"

}
