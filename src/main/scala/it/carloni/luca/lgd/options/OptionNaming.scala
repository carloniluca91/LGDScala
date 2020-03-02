package it.carloni.luca.lgd.options

import it.carloni.luca.lgd.common.utils.LGDCommons

object OptionNaming {

  final val DataDaShortOption = "dd"
  final val DataDaLongOption = "data-da"
  final val DataDaOptionDescription = f"parametro $$data_da (formato ${LGDCommons.DatePatterns.DataDaPattern})"

  final val DataAShortOption = "da"
  final val DataALongOption = "data-a"
  final val DataAOptionDescription = f"parametro $$data_a (formato ${LGDCommons.DatePatterns.DataAPattern})"

  final val DataOsservazioneShortOption = "dao"
  final val DataOsservazioneLongOption = "data-osservazione"
  final val DataOsservazioneOptionDescription = f"parametro $$data_osservazione (formato ${LGDCommons.DatePatterns.DataOsservazionePattern})"

  final val UfficioShortOption = "uf"
  final val UfficioLongOption = "ufficio"
  final val UfficioOptionDescription = "parametro $ufficio"

  final val NumeroMesi1ShortOption = "nm1"
  final val NumeroMesi1LongOption = "numero-mesi-1"
  final val NumeroMesi1Description = "parametro $numero_mesi_1"

  final val NumeroMesi2ShortOption = "nm2"
  final val NumeroMesi2LongOption = "numero-mesi-2"
  final val NumeroMesi2Description = "parametro $numero_mesi_2"

  final val PeriodoShortOption = "pe"
  final val PeriodoLongOption = "periodo"
  final val PeriodoOptionDescription = f"parametro $$periodo (formato ${LGDCommons.DatePatterns.PeriodoPattern}"

}
