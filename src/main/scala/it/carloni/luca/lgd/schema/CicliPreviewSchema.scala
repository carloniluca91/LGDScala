package it.carloni.luca.lgd.schema

import scala.collection.mutable

object CicliPreviewSchema {

  val fposiLoadPigSchema: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap(

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
