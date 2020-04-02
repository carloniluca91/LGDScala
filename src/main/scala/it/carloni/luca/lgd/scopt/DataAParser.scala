package it.carloni.luca.lgd.scopt

import it.carloni.luca.lgd.common.utils.LGDCommons
import it.carloni.luca.lgd.scopt.option.{OptionNaming, OptionValidation}
import scopt.OptionParser

object DataAParser {

  case class DataAConfig(dataA: String = null)

  val optionParser: OptionParser[DataAConfig] = new OptionParser
    [DataAConfig](LGDCommons.Scopt.scoptProgramName) {

    // DATA_A
    opt[String](OptionNaming.DataALongOption)
      .required
      .validate((inputDataA: String) => if (OptionValidation.validateDataA(inputDataA)) success
      else failure(OptionNaming.DataAFailedValidationMessage + inputDataA + ")"))
      .action((inputDataA, config) => config.copy(dataA = inputDataA))
  }
}
