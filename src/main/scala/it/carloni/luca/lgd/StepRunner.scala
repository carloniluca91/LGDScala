package it.carloni.luca.lgd

import it.carloni.luca.lgd.scopt.parser.StepParser
import it.carloni.luca.lgd.spark.common.SparkEnums.StepNames
import org.apache.log4j.Logger

class StepRunner {

  private val logger = Logger.getLogger(getClass)

  def run(args: Array[String], stepName: String): Unit = {

    logger.info(s"Input step name: $stepName")

    val stepNameToUpperCase: String = stepName.toUpperCase
    val stepNameValue: StepNames.Value = StepNames.withName(stepNameToUpperCase)
    stepNameValue match {

      case StepNames.CiclilavStep1 =>

        import it.carloni.luca.lgd.spark.step.CiclilavStep1
        import it.carloni.luca.lgd.scopt.config.DtDaDtAConfig

        logger.info(s"Matched step ${stepNameValue.toString}")

        val dtDaDtAConfig = DtDaDtAConfig
        val optionParser = StepParser.dtDaDtAParser
        optionParser.parse(args, dtDaDtAConfig()) match {

          case Some(value) =>

            logger.info(s"Successfully parsed arguments for step $stepNameToUpperCase")
            new CiclilavStep1().run(value)

          case None => // arguments are bad, error message will have been displayed
        }

      case StepNames.CicliPreview =>

        import it.carloni.luca.lgd.spark.step.CicliPreview
        import it.carloni.luca.lgd.scopt.config.DtAUfficioConfig

        logger.info(s"Matched step ${stepNameValue.toString}")

        val dtAUfficioConfig = DtAUfficioConfig
        val optionParser = StepParser.dtAUfficioParser
        optionParser.parse(args, dtAUfficioConfig()) match {

          case Some(value) =>

            logger.info(s"Successfully parsed arguments for step $stepNameToUpperCase")
            new CicliPreview().run(value)

          case None => // arguments are bad, error message will have been displayed
        }
    }
  }
}
