package it.carloni.luca.lgd.schema

import scala.collection.immutable.Map

object CicliPreviewSchema {

  val fposiLoadPigSchema: Map[String, String] = Map(

    "codicebanca" -> "chararray",
    "ndgprincipale" -> "chararray",
    "datainiziodef" -> "chararray",
    "datafinedef" -> "chararray",
    "datainiziopd" -> "chararray",
    "datainizioinc" -> "chararray",
    "datainizioristrutt" -> "chararray",
    "datasofferenza" -> "chararray",
    "totaccordatodatdef" -> "double",
    "totutilizzdatdef" -> "double",
    "segmento" -> "chararray",
    "naturagiuridica_segm" -> "chararray"
  )
}
