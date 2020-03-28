package it.carloni.luca.lgd

import it.carloni.luca.lgd.scopt.DataANumeroMesi12Parser
import it.carloni.luca.lgd.step.FanagMonthly

object FanagMonthlyApp extends App {

  val dataANumeroMesi12Config = DataANumeroMesi12Parser.Config
  val optionParser = DataANumeroMesi12Parser.optionParser

  optionParser.parse(args, dataANumeroMesi12Config()) match {

    case Some(dataANumeroMesi12Config) =>

      val fanagMonthly = new FanagMonthly(dataANumeroMesi12Config)
      fanagMonthly.run()

    case None => // arguments are bad, error message will have been displayed
  }

}
