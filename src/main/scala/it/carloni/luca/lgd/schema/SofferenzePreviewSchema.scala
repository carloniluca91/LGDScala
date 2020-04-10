package it.carloni.luca.lgd.schema

import scala.collection.immutable.Map

object SofferenzePreviewSchema {

  val soffLoadPigSchema: Map[String, String] = Map(

    "istituto" -> "chararray",
    "ndg" -> "chararray",
    "numerosofferenza" -> "chararray",
    "datainizio" -> "chararray",
    "datafine" -> "chararray",
    "statopratica" -> "chararray",
    "saldoposizione" -> "chararray",
    "saldoposizionecontab" -> "chararray"
  )
}
