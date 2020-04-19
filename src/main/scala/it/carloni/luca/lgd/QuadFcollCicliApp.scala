package it.carloni.luca.lgd

import it.carloni.luca.lgd.scopt.parser.UfficioParser
import it.carloni.luca.lgd.spark.step.QuadFcollCicli

object QuadFcollCicliApp extends App {

  val ufficioConfig = UfficioParser.UfficioConfig
  val optionParser = UfficioParser.optionParser

  optionParser.parse(args, ufficioConfig()) match {

    case Some(ufficioConfig) =>

      new QuadFcollCicli(ufficioConfig.ufficio).run()

    case None => // arguments are bad, error message will have been displayed
  }
}
