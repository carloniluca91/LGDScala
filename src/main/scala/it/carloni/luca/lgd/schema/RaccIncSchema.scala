package it.carloni.luca.lgd.schema

import scala.collection.mutable

object RaccIncSchema {

  val tlbmignPigSchema: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap(

    "cd_isti_ced" -> "chararray",
    "ndg_ced" -> "chararray",
    "cd_abi_ced" -> "chararray",
    "cd_isti_ric" -> "chararray",
    "ndg_ric" -> "chararray",
    "cd_abi_ric" -> "chararray",
    "fl01_anag" -> "chararray",
    "fl02_anag" -> "chararray",
    "fl03_anag" -> "chararray",
    "fl04_anag" -> "chararray",
    "fl05_anag" -> "chararray",
    "fl06_anag" -> "chararray",
    "fl07_anag" -> "chararray",
    "fl08_anag" -> "chararray",
    "fl09_anag" -> "chararray",
    "fl10_anag" -> "chararray",
    "cod_migraz" -> "chararray",
    "data_migraz" -> "chararray",
    "data_ini_appl" -> "chararray",
    "data_fin_appl" -> "chararray",
    "data_ini_appl2" -> "chararray"
  )
}
