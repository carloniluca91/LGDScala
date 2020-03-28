package it.carloni.luca.lgd.scopt

import it.carloni.luca.lgd.scopt.option.{OptionNaming, OptionValidation}
import scopt.OptionParser

object DataANumeroMesi12Parser {

  case class Config(dataA: String = null,
                    numeroMesi1: Int = 0,
                    numeroMesi2: Int = 0)

  val optionParser: OptionParser[Config] = new OptionParser[Config]("scopt 3.x") {

    opt[String](OptionNaming.DataALongOption)
      .required
      .validate((inputDataA: String) => if (OptionValidation.validateDataA(inputDataA)) success
      else failure(OptionNaming.DataAFailedValidationMessage + inputDataA + ")"))
      .action((inputDataA, config) => config.copy(dataA = inputDataA))

    opt[Int](OptionNaming.NumeroMesi1LongOption)
      .required
      .action((numeroMesi1Input, config) => config.copy(numeroMesi1 = numeroMesi1Input))

    opt[Int](OptionNaming.NumeroMesi2LongOption)
      .required
      .action((numeroMesi2Input, config) => config.copy(numeroMesi2 = numeroMesi2Input))
  }
}
