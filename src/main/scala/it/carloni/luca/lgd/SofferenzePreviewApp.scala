package it.carloni.luca.lgd

import it.carloni.luca.lgd.scopt.parser.DataAUfficioParser
import it.carloni.luca.lgd.spark.step.SofferenzePreview

object SofferenzePreviewApp extends App {

  val dataAUfficioConfig = DataAUfficioParser.DataAUfficioConfig
  val optionParser = DataAUfficioParser.optionParser

  optionParser.parse(args, dataAUfficioConfig()) match {

    case Some(dataAUfficioConfig) =>

      val dataA = dataAUfficioConfig.dataA
      val ufficio = dataAUfficioConfig.ufficio

      new SofferenzePreview(dataA, ufficio).run()

    case None => // arguments are bad, error message will have been displayed
  }
}
