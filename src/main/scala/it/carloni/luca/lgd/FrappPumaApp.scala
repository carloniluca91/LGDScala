package it.carloni.luca.lgd

import it.carloni.luca.lgd.scopt.parser.DataAParser
import it.carloni.luca.lgd.spark.step.FrappPuma

object FrappPumaApp extends App {

  val dataAConfig = DataAParser.DataAConfig
  val optionParser = DataAParser.optionParser

  optionParser.parse(args, dataAConfig()) match {

    case Some(dataAConfig) =>

      new FrappPuma(dataAConfig.dataA).run()

    case None => // arguments are bad, error message will have been displayed
  }

}
