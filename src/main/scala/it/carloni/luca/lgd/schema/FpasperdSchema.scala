package it.carloni.luca.lgd.schema

import scala.collection.immutable.Map

object FpasperdSchema {

  val tlbcidefLoadPigSchema: Map[String, String] = Map(

    "codicebanca" -> "chararray",
    "ndgprincipale" -> "chararray",
    "datainiziodef" -> "int",
    "datafinedef" -> "int",
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
    "ndg_collegato" -> "chararray",
    "codicebanca_collegato" -> "chararray",
    "cd_collegamento" -> "chararray",
    "cd_fiscale" -> "chararray"
  )

  val tlbpaspeFilterPigSchema: Map[String, String] = Map(

    "cd_istituto" -> "chararray",
    "ndg" -> "chararray",
    "datacont" -> "int",
    "causale" -> "chararray",
    "importo" -> "chararray"
  )

  val tlbpaspeossPigSchema: Map[String, String] = Map(

    "cd_istituto" -> "chararray",
    "ndg" -> "chararray",
    "datacont" -> "int",
    "causale" -> "chararray",
    "importo" -> "chararray"
  )
}