package it.carloni.luca.lgd

import it.carloni.luca.lgd.scopt.parser.DataANumeroMesi12Parser
import it.carloni.luca.lgd.step.FanagMonthly

object FanagMonthlyApp extends App {

  val dataANumeroMesi12Config = DataANumeroMesi12Parser.DataANumeroMesi12Config
  val optionParser = DataANumeroMesi12Parser.optionParser

  optionParser.parse(args, dataANumeroMesi12Config()) match {

    case Some(dataANumeroMesi12Config) =>

      new FanagMonthly(dataANumeroMesi12Config).run()

    case None => // arguments are bad, error message will have been displayed
  }
}
