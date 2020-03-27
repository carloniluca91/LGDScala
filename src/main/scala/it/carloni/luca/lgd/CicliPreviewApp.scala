package it.carloni.luca.lgd

import it.carloni.luca.lgd.scopt.DataAUfficioParser
import it.carloni.luca.lgd.step.CicliPreview

object CicliPreviewApp extends App {

  val dataAUfficioConfig = DataAUfficioParser.Config
  val optionParser = DataAUfficioParser.optionParser

  optionParser.parse(args, dataAUfficioConfig()) match {

    case Some(dataAUfficioConfig) =>

      val cicliPreview = new CicliPreview(dataAUfficioConfig)
      cicliPreview.run()

    case None => // arguments are bad, error message will have been displayed
  }
}
