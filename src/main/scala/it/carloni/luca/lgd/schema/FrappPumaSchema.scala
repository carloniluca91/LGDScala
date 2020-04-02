package it.carloni.luca.lgd.schema

import scala.collection.immutable.Map

object FrappPumaSchema {

  val tlbcidefPigSchema: Map[String, String] = Map(

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
    "cd_fiscale" -> "chararray",
    "dt_rif_udct" -> "int"
  )

  val tlbgaranPigSchema: Map[String, String] = Map(

    "cd_istituto" -> "chararray",
    "ndg" -> "chararray",
    "sportello" -> "chararray",
    "dt_riferimento" -> "int",
    "conto_esteso" -> "chararray",
    "cd_puma2" -> "chararray",
    "ide_garanzia" -> "chararray",
    "importo" -> "chararray",
    "fair_value" -> "chararray"
  )
}
