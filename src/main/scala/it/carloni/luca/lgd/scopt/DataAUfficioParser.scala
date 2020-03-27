package it.carloni.luca.lgd.scopt

import it.carloni.luca.lgd.scopt.option.{OptionNaming, OptionValidation}
import scopt.OptionParser

object DataAUfficioParser {

  case class Config(ufficio: String = null,
                    dataA: String = null)

  val optionParser: OptionParser[Config] = new OptionParser[Config]("scopt 3.x") {

    opt[String](OptionNaming.UfficioLongOption)
      .required()
      .action((inputUfficio, config) => config.copy(ufficio = inputUfficio))

    opt[String](OptionNaming.DataALongOption)
      .required()
      .validate((inputDataA: String) => if (OptionValidation.validateDataA(inputDataA)) success
      else failure(OptionNaming.DataAFailedValidationMessage + inputDataA + ")"))
      .action((inputDataA, config) => config.copy(dataA = inputDataA))
  }
}
