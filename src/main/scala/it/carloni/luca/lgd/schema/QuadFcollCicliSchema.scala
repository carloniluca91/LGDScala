package it.carloni.luca.lgd.schema

import scala.collection.immutable.Map

object QuadFcollCicliSchema {

  val fcollPigSchema: Map[String, String] = Map(

    "codicebanca" -> "chararray",
    "ndgprincipale" -> "chararray",
    "datainiziodef" -> "chararray",
    "datafinedef" -> "chararray",
    "data_default" -> "chararray",
    "istituto_collegato" -> "chararray",
    "ndg_collegato" -> "chararray",
    "data_collegamento" -> "chararray",
    "cumulo" -> "chararray"
  )

  val cicliNdgLoadPigSchema: Map[String, String] = Map(

    "cd_isti" -> "chararray",
    "ndg_principale" -> "chararray",
    "dt_inizio_ciclo" -> "chararray",
    "dt_fine_ciclo" -> "chararray",
    "datainiziopd" -> "chararray",
    "datainizioristrutt" -> "chararray",
    "datainizioinc" -> "chararray",
    "datainiziosoff" -> "chararray",
    "c_key" -> "chararray",
    "tipo_segmne" -> "chararray",
    "sae_segm" -> "chararray",
    "rae_segm" -> "chararray",
    "segmento" -> "chararray",
    "tp_ndg" -> "chararray",
    "provincia_segm" -> "chararray",
    "databilseg" -> "chararray",
    "strbilseg" -> "chararray",
    "attivobilseg" -> "chararray",
    "fatturbilseg" -> "chararray",
    "ndg_coll" -> "chararray",
    "cd_isti_coll" -> "chararray",
  )
}
