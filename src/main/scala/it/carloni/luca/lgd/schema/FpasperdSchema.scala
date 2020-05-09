package it.carloni.luca.lgd.schema

import scala.collection.mutable

object FpasperdSchema {

  val tlbcidefLoadPigSchema: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap(

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

  val tlbpaspeFilterPigSchema: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap(

    "cd_istituto" -> "chararray",
    "ndg" -> "chararray",
    "datacont" -> "int",
    "causale" -> "chararray",
    "importo" -> "chararray"
  )

  val tlbpaspeossPigSchema: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap(

    "cd_istituto" -> "chararray",
    "ndg" -> "chararray",
    "datacont" -> "int",
    "causale" -> "chararray",
    "importo" -> "chararray"
  )
}
