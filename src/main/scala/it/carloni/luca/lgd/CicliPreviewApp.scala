package it.carloni.luca.lgd

import it.carloni.luca.lgd.scopt.parser.DataAUfficioParser
import it.carloni.luca.lgd.step.CicliPreview

object CicliPreviewApp extends App {

  val dataAUfficioConfig = DataAUfficioParser.DataAUfficioConfig
  val optionParser = DataAUfficioParser.optionParser

  optionParser.parse(args, dataAUfficioConfig()) match {

    case Some(dataAUfficioConfig) =>

      new CicliPreview(dataAUfficioConfig).run()

    case None => // arguments are bad, error message will have been displayed
  }
}
