package it.carloni.luca.lgd.schema

import scala.collection.immutable.Map

object CiclilavStep1Schema {

  val tlbcidefPigSchema: Map[String, String] = Map(

    "cd_isti" -> "chararray",
    "ndg_principale" -> "chararray",
    "cod_cr" -> "chararray",
    "dt_inizio_ciclo" -> "int",
    "dt_ingresso_status" -> "int",
    "status_ingresso" -> "chararray",
    "dt_uscita_status" -> "chararray",
    "status_uscita" -> "chararray",
    "dt_fine_ciclo" -> "chararray",
    "indi_pastdue" -> "chararray",
    "indi_impr_priv" -> "chararray"
  )

  val tlbcraccPigSchema: Map[String, String] = Map(

    "data_rif" -> "int",
    "cd_isti" -> "chararray",
    "ndg" -> "chararray",
    "cod_raccordo" -> "chararray",
    "data_val" -> "int"
  )

}
