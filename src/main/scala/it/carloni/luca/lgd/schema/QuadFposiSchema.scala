package it.carloni.luca.lgd.schema

import scala.collection.immutable.Map

object QuadFposiSchema {

  val hadoopFposiPigSchema: Map[String, String] = Map(

    "codicebanca" -> "chararray",
    "ndgprincipale" -> "chararray",
    "datainiziodef" -> "chararray",
    "datafinedef" -> "chararray",
    "ndg_gruppo" -> "chararray",
    "datainiziopd" -> "chararray",
    "datainizioinc" -> "chararray",
    "datainizioristrutt" -> "chararray",
    "datainiziosoff" -> "chararray",
    "totaccordatodatdef" -> "chararray",
    "totutilizzdatdef" -> "chararray",
    "naturagiuridica_segm" -> "chararray",
    "intestazione" -> "chararray",
    "codicefiscale_segm" -> "chararray",
    "partitaiva_segm" -> "chararray",
    "sae_segm" -> "chararray",
    "rae_segm" -> "chararray",
    "ciae_ndg" -> "chararray",
    "provincia_segm" -> "chararray",
    "ateco" -> "chararray",
    "segmento" -> "chararray",
    "databilseg" -> "chararray",
    "strbilseg" -> "chararray",
    "attivobilseg" -> "chararray",
    "fatturbilseg" -> "chararray"
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
