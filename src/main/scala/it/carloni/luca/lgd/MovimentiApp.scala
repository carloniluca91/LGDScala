package it.carloni.luca.lgd

import it.carloni.luca.lgd.scopt.parser.DataOsservazioneParser
import it.carloni.luca.lgd.spark.step.Movimenti

object MovimentiApp extends App {

  val dataOsservazioneConfig = DataOsservazioneParser.DataOsservazioneConfig
  val optionParser = DataOsservazioneParser.optionParser

  optionParser.parse(args, dataOsservazioneConfig()) match {

    case Some(dataOsservazioneConfig) =>

      new Movimenti(dataOsservazioneConfig.dataOsservazione).run()

    case None => // arguments are bad, error message will have been displayed
  }

}
