package it.carloni.luca.lgd

import it.carloni.luca.lgd.scopt.config._
import it.carloni.luca.lgd.scopt.parser.StepParser
import it.carloni.luca.lgd.spark.common.SparkEnums.StepNames
import it.carloni.luca.lgd.spark.step._
import org.apache.log4j.Logger

class StepRunner {

  private val logger = Logger.getLogger(getClass)

  def run(args: Array[String], stepName: String): Unit = {

    logger.info(s"Input step name: $stepName")

    val stepNameToUpperCase: String = stepName.toUpperCase
    val stepNameValue: StepNames.Value = StepNames.withName(stepNameToUpperCase)

    stepNameValue match {

      case StepNames.CiclilavStep1 =>

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

        logger.info(s"Matched step ${stepNameValue.toString}")

        val dtAUfficioConfig = DtAUfficioConfig
        val optionParser = StepParser.dtAUfficioParser
        optionParser.parse(args, dtAUfficioConfig()) match {

          case Some(value) =>

            logger.info(s"Successfully parsed arguments for step $stepNameToUpperCase")
            new CicliPreview().run(value)

          case None => // arguments are bad, error message will have been displayed
        }

      case StepNames.FanagMonthly =>

        logger.info(s"Matched step ${stepNameValue.toString}")

        val dtANumeroMesi12Config = DtANumeroMesi12Config
        val optionParser = StepParser.dtANumeroMesi1And2Parser
        optionParser.parse(args, dtANumeroMesi12Config()) match {

          case Some(value) =>

            logger.info(s"Successfully parsed arguments for step $stepNameToUpperCase")
            new FanagMonthly().run(value)

          case None => // arguments are bad, error message will have been displayed
        }

      case StepNames.Fpasperd =>

        logger.info(s"Matched step ${stepNameValue.toString}")

        new Fpasperd().run(new EmptyConfig())

      case StepNames.FrappNdgMonthly =>

        logger.info(s"Matched step ${stepNameValue.toString}")

        val dtANumeroMesi12Config = DtANumeroMesi12Config
        val optionParser = StepParser.dtANumeroMesi1And2Parser
        optionParser.parse(args, dtANumeroMesi12Config()) match {

          case Some(value) =>

            logger.info(s"Successfully parsed arguments for step $stepNameToUpperCase")
            new FrappNdgMonthly().run(value)

          case None => // arguments are bad, error message will have been displayed
        }

      case StepNames.FrappPuma =>

        logger.info(s"Matched step ${stepNameValue.toString}")

        val dtAConfig = DtAConfig
        val optionParser = StepParser.dtAParser
        optionParser.parse(args, dtAConfig()) match {

          case Some(value) =>

            logger.info(s"Successfully parsed arguments for step $stepNameToUpperCase")
            new FrappPuma().run(value)

          case None => // arguments are bad, error message will have been displayed
        }

      case StepNames.Movimenti =>

        logger.info(s"Matched step ${stepNameValue.toString}")

        val dtOsservazioneConfig = DtOsservazioneConfig
        val optionParser = StepParser.dtOsservazioneParser
        optionParser.parse(args, dtOsservazioneConfig()) match {

          case Some(value) =>

            logger.info(s"Successfully parsed arguments for step $stepNameToUpperCase")
            new Movimenti().run(value)

          case None => // arguments are bad, error message will have been displayed
        }

      case StepNames.Posaggr =>

        logger.info(s"Matched step ${stepNameValue.toString}")

        new Posaggr().run(new EmptyConfig())

      case StepNames.QuadFcoll =>

        logger.info(s"Matched step ${stepNameValue.toString}")

        new QuadFcoll().run(new EmptyConfig())

      case StepNames.QuadFcollCicli =>

        logger.info(s"Matched step ${stepNameValue.toString}")

        val ufficioConfig = UfficioConfig
        val optionParser = StepParser.ufficioParser

        optionParser.parse(args, ufficioConfig()) match {

          case Some(value) =>

            logger.info(s"Successfully parsed arguments for step $stepNameToUpperCase")
            new QuadFcollCicli().run(value)

          case None => // arguments are bad, error message will have been displayed
        }

      case StepNames.QuadFposi =>

        logger.info(s"Matched step ${stepNameValue.toString}")

        val ufficioConfig = UfficioConfig
        val optionParser = StepParser.ufficioParser

        optionParser.parse(args, ufficioConfig()) match {

          case Some(value) =>

            logger.info(s"Successfully parsed arguments for step $stepNameToUpperCase")
            new QuadFposi().run(value)

          case None => // arguments are bad, error message will have been displayed
        }

      case StepNames.QuadFrapp =>

        logger.info(s"Matched step ${stepNameValue.toString}")

        val ufficioConfig = UfficioConfig
        val optionParser = StepParser.ufficioParser

        optionParser.parse(args, ufficioConfig()) match {

          case Some(value) =>

            logger.info(s"Successfully parsed arguments for step $stepNameToUpperCase")
            new QuadFrapp().run(value)

          case None => // arguments are bad, error message will have been displayed
        }

      case StepNames.RaccInc =>

        logger.info(s"Matched step ${stepNameValue.toString}")
        new RaccInc().run(new EmptyConfig())

      case StepNames.SofferenzePreview =>

        logger.info(s"Matched step ${stepNameValue.toString}")

        val dtAUfficioConfig = DtAUfficioConfig
        val optionParser = StepParser.dtAUfficioParser

        optionParser.parse(args, dtAUfficioConfig()) match {

          case Some(value) =>

            logger.info(s"Successfully parsed arguments for step $stepNameToUpperCase")
            new SofferenzePreview().run(value)

          case None => // arguments are bad, error message will have been displayed
        }

      case _ => logger.warn(s"Unable to match step $stepNameToUpperCase. Thus, no step will be run")
    }
  }
}
