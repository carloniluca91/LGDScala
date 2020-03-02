package it.carloni.luca.lgd.params

import org.apache.commons.cli.{CommandLine, DefaultParser, Option, Options}
import org.apache.log4j.Logger

import scala.collection.mutable

class LgdParamsParser(private val args: Array[String], private val optionList: List[Option]){

  private val logger: Logger = Logger.getLogger(getClass)
  private val stepParamsOptions: Options = new Options
  private val stepParamsMap = new mutable.HashMap[String, String]

  commandLineParsing()

  def getStepParamsMap: mutable.Map[String, String] = stepParamsMap

  private def commandLineParsing(){

    optionList foreach {stepParamsOptions.addOption}
    val commandLineParser = new DefaultParser
    val commandLine = commandLineParser.parse(stepParamsOptions, args)

    optionList foreach {enrichStepParamsMap(_, commandLine)}
    logger.info("Arguments parsed correctly")
  }

  private def enrichStepParamsMap(option: Option, commandLine: CommandLine): Unit = {

    if (stepParamsOptions.hasLongOption(option.getLongOpt)) {

      stepParamsMap(option.getLongOpt) = commandLine.getOptionValue(option.getLongOpt)
      logger.debug(f"${option.getDescription}: ${stepParamsMap(option.getLongOpt)}")
    }
  }
}
