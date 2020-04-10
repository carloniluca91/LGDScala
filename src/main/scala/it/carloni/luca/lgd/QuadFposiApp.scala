package it.carloni.luca.lgd

import it.carloni.luca.lgd.scopt.parser.UfficioParser
import it.carloni.luca.lgd.step.QuadFposi

object QuadFposiApp extends App {

  val ufficioConfig = UfficioParser.UfficioConfig
  val optionParser = UfficioParser.optionParser

  optionParser.parse(args, ufficioConfig()) match {

    case Some(ufficioConfig) =>

      new QuadFposi(ufficioConfig.ufficio).run()

    case None => // arguments are bad, error message will have been displayed
  }

}
