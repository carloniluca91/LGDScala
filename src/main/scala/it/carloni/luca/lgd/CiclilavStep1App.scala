package it.carloni.luca.lgd

import it.carloni.luca.lgd.scopt.parser.DataDaDataAParser
import it.carloni.luca.lgd.step.CiclilavStep1

object CiclilavStep1App extends App {

  val dataDaDataAConfig = DataDaDataAParser.DataDaDataAConfig
  val optionParser = DataDaDataAParser.optionParser

  optionParser.parse(args, dataDaDataAConfig()) match {

    case Some(dataDaDataAConfig) =>

      val dataDa = dataDaDataAConfig.dataDa
      val dataA = dataDaDataAConfig.dataA

      new CiclilavStep1(dataDa, dataA).run()

    case None => // arguments are bad, error message will have been displayed
  }
}
