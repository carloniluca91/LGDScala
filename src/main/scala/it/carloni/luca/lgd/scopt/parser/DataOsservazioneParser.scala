package it.carloni.luca.lgd.scopt.parser

import it.carloni.luca.lgd.commons.LGDCommons
import it.carloni.luca.lgd.scopt.option.{OptionNaming, OptionValidation}
import scopt.OptionParser

object DataOsservazioneParser {

  case class DataOsservazioneConfig(dataOsservazione: String = null)

  val optionParser: OptionParser[DataOsservazioneConfig] = new OptionParser
    [DataOsservazioneConfig](LGDCommons.Scopt.scoptProgramName) {

    // DATA_OSSERVAZIONE
    opt[String](OptionNaming.DataOsservazioneLongOption)
      .required()
      .validate((inputDataOsservazione: String) => if (OptionValidation.validateDataOsservazione(inputDataOsservazione)) success
      else failure(OptionNaming.DataOsservazioneFailedValidationMessage + inputDataOsservazione + ")"))
      .action((inputDataOsservazione, config) => config.copy(dataOsservazione = inputDataOsservazione))

  }

}
