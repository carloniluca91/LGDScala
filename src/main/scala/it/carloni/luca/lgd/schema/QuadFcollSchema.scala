package it.carloni.luca.lgd.schema

import scala.collection.mutable

object QuadFcollSchema {

  val fcollLoadPigSchema: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap(

    "cumulo" -> "chararray",
    "cd_istituto_COLL" -> "chararray",
    "ndg_COLL" -> "chararray",
    "data_inizio_DEF" -> "chararray",
    "data_collegamento" -> "chararray",
    "pri" -> "chararray" )

  val oldFposiLoadPigSchema: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap(

    "datainizioDEF" -> "chararray",
    "dataFINEDEF" -> "chararray",
    "dataINIZIOPD" -> "chararray",
    "datainizioinc" -> "chararray",
    "dataSOFFERENZA" -> "chararray",
    "codicebanca" -> "chararray",
    "ndgprincipale" -> "chararray",
    "flagincristrut" -> "chararray",
    "cumulo" -> "chararray")
}
