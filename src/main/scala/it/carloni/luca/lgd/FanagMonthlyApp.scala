package it.carloni.luca.lgd

import it.carloni.luca.lgd.scopt.parser.DataANumeroMesi12Parser
import it.carloni.luca.lgd.step.FanagMonthly

object FanagMonthlyApp extends App {

  val dataANumeroMesi12Config = DataANumeroMesi12Parser.DataANumeroMesi12Config
  val optionParser = DataANumeroMesi12Parser.optionParser

  optionParser.parse(args, dataANumeroMesi12Config()) match {

    case Some(dataANumeroMesi12Config) =>

      val dataA = dataANumeroMesi12Config.dataA
      val numeroMesi1 = dataANumeroMesi12Config.numeroMesi1
      val numeroMesi2 = dataANumeroMesi12Config.numeroMesi2

      new FanagMonthly(dataA, numeroMesi1, numeroMesi2).run()

    case None => // arguments are bad, error message will have been displayed
  }
}
