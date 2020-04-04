package it.carloni.luca.lgd.schema

import scala.collection.immutable.Map

object QuadFcollSchema {

  val fcollLoadPigSchema: Map[String, String] = Map(

    "cumulo" -> "chararray",
    "cd_istituto_COLL" -> "chararray",
    "ndg_COLL" -> "chararray",
    "data_inizio_DEF" -> "chararray",
    "data_collegamento" -> "chararray",
    "pri" -> "chararray",
  )

  val oldFposiLoadPigSchema: Map[String, String] = Map(

    "datainizioDEF" -> "chararray",
    "dataFINEDEF" -> "chararray",
    "dataINIZIOPD" -> "chararray",
    "datainizioinc" -> "chararray",
    "dataSOFFERENZA" -> "chararray",
    "codicebanca" -> "chararray",
    "ndgprincipale" -> "chararray",
    "flagincristrut" -> "chararray",
    "cumulo" -> "chararray"
  )

}
