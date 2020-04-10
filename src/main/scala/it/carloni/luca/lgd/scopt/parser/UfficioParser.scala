package it.carloni.luca.lgd.scopt.parser

import it.carloni.luca.lgd.commons.LGDCommons
import it.carloni.luca.lgd.scopt.option.OptionNaming
import scopt.OptionParser

object UfficioParser {

  case class UfficioConfig(ufficio: String = null)

  val optionParser: OptionParser[UfficioConfig] = new OptionParser
    [UfficioConfig](LGDCommons.Scopt.scoptProgramName) {

    // UFFICIO
    opt[String](OptionNaming.UfficioLongOption)
      .required
      .action((inputUfficio, config) => config.copy(ufficio = inputUfficio))
  }
}
