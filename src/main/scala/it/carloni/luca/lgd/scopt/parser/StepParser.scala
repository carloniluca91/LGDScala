package it.carloni.luca.lgd.scopt.parser

import it.carloni.luca.lgd.scopt.config.{DtAConfig, DtANumeroMesi12Config, DtAUfficioConfig, DtDaDtAConfig, DtOsservazioneConfig, UfficioConfig}
import it.carloni.luca.lgd.scopt.option.{OptionNaming, OptionValidation}
import scopt.OptionParser

object StepParser {

  val scoptProgramName = "scopt 3.3.0"

  case class StepNameConfig(stepName: String = null)

  val stepNameOptionParser: OptionParser[StepNameConfig] = new OptionParser
    [StepNameConfig](scoptProgramName) {

    // DO NOT FAIL ON UNKNOWN ARGUMENTS AND DO NOT SHOW WARNING
    override def errorOnUnknownArgument = false
    override def reportWarning(msg: String): Unit = {}

    // STEP_NAME
    opt[String](OptionNaming.StepNameShortOption, OptionNaming.StepNameLongOption)
      .text(OptionNaming.StepNameOptionDescription)
      .required
      .action((inputStepName, config) => config.copy(stepName = inputStepName))
  }

  val dtDaDtAParser: OptionParser[DtDaDtAConfig] = new OptionParser[DtDaDtAConfig](scoptProgramName) {

    override def errorOnUnknownArgument = false
    override def reportWarning(msg: String): Unit = {}

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

  val dtANumeroMesi1And2Parser: OptionParser[DtANumeroMesi12Config] = new OptionParser[DtANumeroMesi12Config](scoptProgramName) {

    override def errorOnUnknownArgument = false
    override def reportWarning(msg: String): Unit = {}

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

  val dtAParser: OptionParser[DtAConfig] = new OptionParser[DtAConfig](scoptProgramName) {

    override def errorOnUnknownArgument = false
    override def reportWarning(msg: String): Unit = {}

    // DATA_A
    opt[String](OptionNaming.DataALongOption)
      .required
      .validate((inputDataA: String) => if (OptionValidation.validateDataA(inputDataA)) success
      else failure(OptionNaming.DataAFailedValidationMessage + inputDataA + ")"))
      .action((inputDataA, config) => config.copy(dataA = inputDataA))
  }

  val dtAUfficioParser: OptionParser[DtAUfficioConfig] = new OptionParser[DtAUfficioConfig](scoptProgramName) {

    override def errorOnUnknownArgument = false
    override def reportWarning(msg: String): Unit = {}

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

  val dtOsservazioneParser: OptionParser[DtOsservazioneConfig] = new OptionParser[DtOsservazioneConfig](scoptProgramName) {

    override def errorOnUnknownArgument = false
    override def reportWarning(msg: String): Unit = {}

    // DATA_OSSERVAZIONE
    opt[String](OptionNaming.DataOsservazioneLongOption)
      .required()
      .validate((inputDataOsservazione: String) => if (OptionValidation.validateDataOsservazione(inputDataOsservazione)) success
      else failure(OptionNaming.DataOsservazioneFailedValidationMessage + inputDataOsservazione + ")"))
      .action((inputDataOsservazione, config) => config.copy(dataOsservazione = inputDataOsservazione))
  }

  val ufficioParser: OptionParser[UfficioConfig] = new OptionParser[UfficioConfig](scoptProgramName) {

    override def errorOnUnknownArgument = false
    override def reportWarning(msg: String): Unit = {}

    // UFFICIO
    opt[String](OptionNaming.UfficioLongOption)
      .required
      .action((inputUfficio, config) => config.copy(ufficio = inputUfficio))
  }
}
