package it.carloni.luca.lgd.scopt.parser

import it.carloni.luca.lgd.commons.LGDCommons
import it.carloni.luca.lgd.scopt.option.{OptionNaming, OptionValidation}
import scopt.OptionParser

object DataAUfficioParser {

  case class DataAUfficioConfig(ufficio: String = null, dataA: String = null)

  val optionParser: OptionParser[DataAUfficioConfig] = new OptionParser[DataAUfficioConfig](LGDCommons.Scopt.scoptProgramName) {

    // UFFICIO
    opt[String](OptionNaming.UfficioLongOption)
      .required
      .action((inputUfficio, config) => config.copy(ufficio = inputUfficio))

    // DATA_A
    opt[String](OptionNaming.DataALongOption)
      .required
      .validate((inputDataA: String) => if (OptionValidation.validateDataA(inputDataA)) success
      else failure(OptionNaming.DataAFailedValidationMessage + inputDataA + ")"))
      .action((inputDataA, config) => config.copy(dataA = inputDataA))
  }
}
