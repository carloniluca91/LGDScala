package it.carloni.luca.lgd.schema

import scala.collection.immutable.Map

object FrappNdgMonthlySchema {

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

  val tlburttPigSchema: Map[String, String] = Map(

    "cd_istituto" -> "chararray",
    "ndg" -> "chararray",
    "sportello" -> "chararray",
    "conto" -> "chararray",
    "progr_segmento" -> "int",
    "dt_riferimento" -> "int",
    "conto_esteso" -> "chararray",
    "forma_tecnica" -> "chararray",
    "flag_durata_contr" -> "chararray",
    "cred_agevolato" -> "chararray",
    "operazione_pool" -> "chararray",
    "dt_accensione" -> "chararray",
    "dt_estinzione" -> "chararray",
    "dt_scadenza" -> "chararray",
    "organo_deliber" -> "chararray",
    "dt_delibera" -> "chararray",
    "dt_scad_fido" -> "chararray",
    "origine_rapporto" -> "chararray",
    "tp_ammortamento" -> "chararray",
    "tp_rapporto" -> "chararray",
    "period_liquid" -> "chararray",
    "freq_remargining" -> "chararray",
    "cd_prodotto_ris" -> "chararray",
    "durata_originaria" -> "chararray",
    "divisa" -> "chararray",
    "score_erogaz" -> "chararray",
    "durata_residua" -> "chararray",
    "categoria_sof" -> "chararray",
    "categoria_inc" -> "chararray",
    "dur_res_default" -> "chararray",
    "flag_margine" -> "chararray",
    "dt_entrata_def" -> "chararray",
    "tp_contr_rapp" -> "chararray",
    "cd_eplus" -> "chararray",
    "r792_tipocartol" -> "chararray"
  )

}
