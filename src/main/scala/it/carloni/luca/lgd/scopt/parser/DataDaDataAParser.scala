package it.carloni.luca.lgd.scopt.parser

import it.carloni.luca.lgd.commons.LGDCommons
import it.carloni.luca.lgd.scopt.option.{OptionNaming, OptionValidation}
import scopt.OptionParser

object DataDaDataAParser {

  case class DataDaDataAConfig(dataDa: String = null, dataA: String = null)

  val optionParser: OptionParser[DataDaDataAConfig] = new OptionParser[DataDaDataAConfig](LGDCommons.Scopt.scoptProgramName) {

    // DATA_DA
    opt[String](OptionNaming.DataDaLongOption)
      .required()
      .text("parametro $data_da")
      .validate((inputDataDa: String) => if (OptionValidation.validateDataDa(inputDataDa)) success
        else failure(OptionNaming.DataDaFailedValidationMessage + inputDataDa + ")"))
      .action((inputDataDa, config) => config.copy(dataDa = inputDataDa))

    // DATA_A
    opt[String](OptionNaming.DataALongOption)
      .required()
      .text("parametro $data_a")
      .validate((inputDataA: String) => if (OptionValidation.validateDataA(inputDataA)) success
      else failure(OptionNaming.DataAFailedValidationMessage + inputDataA + ")"))
      .action((inputDataA, config) => config.copy(dataA = inputDataA))
  }

}
