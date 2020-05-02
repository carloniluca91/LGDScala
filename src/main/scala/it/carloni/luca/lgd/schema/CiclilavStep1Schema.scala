package it.carloni.luca.lgd.schema

import scala.collection.mutable

object CiclilavStep1Schema {

  val tlbcidefPigSchema: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap(

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

  val tlbcraccPigSchema: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap(

    "data_rif" -> "int",
    "cd_isti" -> "chararray",
    "ndg" -> "chararray",
    "cod_raccordo" -> "chararray",
    "data_val" -> "int"
  )

}
