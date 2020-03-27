package it.carloni.luca.lgd.scopt

import it.carloni.luca.lgd.scopt.option.{OptionNaming, OptionValidation}
import scopt.OptionParser

object DataDaDataAParser {

  case class Config(dataDa: String = null,
                    dataA: String = null)

  val optionParser: OptionParser[Config] = new OptionParser[Config]("scopt 3.x") {

    opt[String](OptionNaming.DataDaLongOption)
      .required()
      .validate((inputDataDa: String) => if (OptionValidation.validateDataDa(inputDataDa)) success
        else failure(OptionNaming.DataDaFailedValidationMessage + inputDataDa + ")"))
      .action((inputDataDa, config) => config.copy(dataDa = inputDataDa))

    opt[String](OptionNaming.DataALongOption)
      .required()
      .validate((inputDataA: String) => if (OptionValidation.validateDataA(inputDataA)) success
      else failure(OptionNaming.DataAFailedValidationMessage + inputDataA + ")"))
      .action((inputDataA, config) => config.copy(dataA = inputDataA))
  }

}
