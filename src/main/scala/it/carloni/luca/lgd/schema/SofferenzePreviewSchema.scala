package it.carloni.luca.lgd.schema

import scala.collection.mutable

object SofferenzePreviewSchema {

  val soffLoadPigSchema: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap(

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
