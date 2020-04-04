package it.carloni.luca.lgd

import it.carloni.luca.lgd.scopt.parser.DataANumeroMesi12Parser
import it.carloni.luca.lgd.step.FrappNdgMonthly

object FrappNdgMonthlyApp extends App {

  val dataANumeroMesi12Config = DataANumeroMesi12Parser.DataANumeroMesi12Config
  val optionParser = DataANumeroMesi12Parser.optionParser

  optionParser.parse(args, dataANumeroMesi12Config()) match {

    case Some(config) =>

      new FrappNdgMonthly(config).run()

    case None => // arguments are bad, error message will have been displayed
  }
}
