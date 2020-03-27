package it.carloni.luca.lgd

import it.carloni.luca.lgd.scopt.DataDaDataAParser
import it.carloni.luca.lgd.step.CiclilavStep1

object CiclilavStep1App extends App {

  val dataDaDataAConfig = DataDaDataAParser.Config
  val optionParser = DataDaDataAParser.optionParser

  optionParser.parse(args, dataDaDataAConfig()) match {

    case Some(dataDaDataAConfig) =>

      val ciclilavStep1 = new CiclilavStep1(dataDaDataAConfig)
      ciclilavStep1.run()

    case None => // arguments are bad, error message will have been displayed
  }
}
