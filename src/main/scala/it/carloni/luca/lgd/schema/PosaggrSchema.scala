package it.carloni.luca.lgd.schema

import scala.collection.immutable.Map

object PosaggrSchema {

  val tblcompPigSchema: Map[String, String] = Map(

    "dt_riferimento" -> "int",
    "c_key" -> "chararray",
    "tipo_segmne" -> "chararray",
    "cd_istituto" -> "chararray",
    "ndg" -> "chararray"
  )

  val tlbaggrPigSchema: Map[String, String] = Map(

    "dt_riferimento" -> "int",
    "c_key_aggr" -> "chararray",
    "ndg_gruppo" -> "chararray",
    "cod_fiscale" -> "chararray",
    "tipo_segmne_aggr" -> "chararray",
    "segmento" -> "chararray",
    "tipo_motore" -> "chararray",
    "cd_istituto" -> "chararray",
    "ndg" -> "chararray",
    "rae" -> "chararray",
    "sae" -> "chararray",
    "tp_ndg" -> "chararray",
    "prov_segm" -> "chararray",
    "fonte_segmento" -> "chararray",
    "utilizzo_cr" -> "chararray",
    "accordato_cr" -> "chararray",
    "databil" -> "chararray",
    "strutbil" -> "chararray",
    "fatturbil" -> "chararray",
    "attivobil" -> "chararray",
    "codimp_cebi" -> "chararray",
    "tot_acco_agr" -> "double",
    "tot_util_agr" -> "chararray",
    "n058_int_vig" -> "chararray"
  )

  val tlbposiPigSchema: Map[String, String] = Map(

    "dt_riferimento" -> "int",
    "cd_istituto" -> "chararray",
    "ndg" -> "chararray",
    "c_key" -> "chararray",
    "cod_fiscale" -> "chararray",
    "ndg_gruppo" -> "chararray",
    "bo_acco" -> "chararray",
    "bo_util" -> "chararray",
    "tot_add_sosp" -> "chararray",
    "tot_val_intr" -> "chararray",
    "ca_acco" -> "chararray",
    "ca_util" -> "chararray",
    "fl_incaglio" -> "chararray",
    "fl_soff" -> "chararray",
    "fl_inc_ogg" -> "chararray",
    "fl_ristr" -> "chararray",
    "fl_pd_90" -> "chararray",
    "fl_pd_180" -> "chararray",
    "util_cassa" -> "chararray",
    "fido_op_cassa" -> "chararray",
    "utilizzo_titoli" -> "chararray",
    "esposizione_titoli" -> "chararray"
  )

}
