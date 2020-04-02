package it.carloni.luca.lgd

import it.carloni.luca.lgd.scopt.DataDaDataAParser
import it.carloni.luca.lgd.step.CiclilavStep1

object CiclilavStep1App extends App {

  val dataDaDataAConfig = DataDaDataAParser.DataDaDataAConfig
  val optionParser = DataDaDataAParser.optionParser

  optionParser.parse(args, dataDaDataAConfig()) match {

    case Some(dataDaDataAConfig) =>

      new CiclilavStep1(dataDaDataAConfig).run()

    case None => // arguments are bad, error message will have been displayed
  }
}
