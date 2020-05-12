package it.carloni.luca.lgd.schema

import scala.collection.mutable

object MovimentiSchema {

  val tlbmovcontaPigSchema: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap(

    "mo_dt_riferimento" -> "chararray",
    "mo_istituto" -> "chararray",
    "mo_ndg" -> "chararray",
    "mo_sportello" -> "chararray",
    "mo_conto" -> "chararray",
    "mo_conto_esteso" -> "chararray",
    "mo_num_soff" -> "chararray",
    "mo_cat_rapp_soff" -> "chararray",
    "mo_fil_rapp_soff" -> "chararray",
    "mo_num_rapp_soff" -> "chararray",
    "mo_id_movimento" -> "chararray",
    "mo_categoria" -> "chararray",
    "mo_causale" -> "chararray",
    "mo_dt_contabile" -> "int",
    "mo_dt_valuta" -> "chararray",
    "mo_imp_movimento" -> "chararray",
    "mo_flag_extracont" -> "chararray",
    "mo_flag_storno" -> "chararray",
    "mo_ndg_principale" -> "chararray",
    "mo_dt_inizio_ciclo" -> "chararray"
  )

}
