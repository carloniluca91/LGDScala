package it.carloni.luca.lgd.scopt.parser

import it.carloni.luca.lgd.commons.LGDCommons
import it.carloni.luca.lgd.scopt.option.{OptionNaming, OptionValidation}
import scopt.OptionParser

object DataANumeroMesi12Parser {

  case class DataANumeroMesi12Config(dataA: String = null, numeroMesi1: Int = 0, numeroMesi2: Int = 0)

  val optionParser: OptionParser[DataANumeroMesi12Config] = new OptionParser[DataANumeroMesi12Config](LGDCommons.Scopt.scoptProgramName) {

    // DATA_A
    opt[String](OptionNaming.DataALongOption)
      .required
      .validate((inputDataA: String) => if (OptionValidation.validateDataA(inputDataA)) success
      else failure(OptionNaming.DataAFailedValidationMessage + inputDataA + ")"))
      .action((inputDataA, config) => config.copy(dataA = inputDataA))

    // NUMERO_MESI_1
    opt[Int](OptionNaming.NumeroMesi1LongOption)
      .required
      .action((numeroMesi1Input, config) => config.copy(numeroMesi1 = numeroMesi1Input))

    // NUMERO_MESI_2
    opt[Int](OptionNaming.NumeroMesi2LongOption)
      .required
      .action((numeroMesi2Input, config) => config.copy(numeroMesi2 = numeroMesi2Input))
  }
}
