package it.carloni.luca.lgd.schema

import scala.collection.mutable

object FanagMonthlySchema {

  val cicliNdgPigSchema: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap (

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

  val tlbuactLoadPigSchema: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap (

    "cd_istituto" -> "chararray",
    "ndg" -> "chararray",
    "dt_riferimento" -> "int",
    "cab_prov_com_stato" -> "chararray",
    "provincia" -> "chararray",
    "sae" -> "chararray",
    "rae" -> "chararray",
    "ciae" -> "chararray",
    "sportello" -> "chararray",
    "area_affari" -> "chararray",
    "tp_ndg" -> "chararray",
    "intestazione" -> "chararray",
    "partita_iva" -> "chararray",
    "cd_fiscale" -> "chararray",
    "ctp_cred_proc_conc" -> "chararray",
    "cliente_protestato" -> "chararray",
    "status_sygei" -> "chararray",
    "ndg_caponucleo" -> "chararray",
    "specie_giuridica" -> "chararray",
    "dt_inizio_rischio" -> "chararray",
    "dt_revoca_fidi" -> "chararray",
    "dt_cens_anagrafe" -> "chararray",
    "cab_luogo_nascita" -> "chararray",
    "dt_costituz_nascit" -> "chararray",
    "tp_contr_basilea1" -> "chararray",
    "flag_fallibile" -> "chararray",
    "stato_controparte" -> "chararray",
    "dt_estinz_ctp" -> "chararray",
    "cliente_fido_rev" -> "chararray",
    "tp_controparte" -> "chararray",
    "grande_sett_attiv" -> "chararray",
    "grande_ramo_attiv" -> "chararray",
    "n058_interm_vigil" -> "chararray",
    "n160_cod_ateco" -> "chararray"
  )

  val tlbudtcPigSchema: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap (

    "cd_istituto" -> "chararray",
    "ndg" -> "chararray",
    "dt_riferimento" -> "int",
    "reddito" -> "chararray",
    "totale_utilizzi" -> "chararray",
    "totale_fidi_delib" -> "chararray",
    "totale_accordato" -> "chararray",
    "patrimonio" -> "chararray",
    "n_dipendenti" -> "chararray",
    "tot_rischi_indir" -> "chararray",
    "status_ndg" -> "chararray",
    "status_basilea2" -> "chararray",
    "posiz_soff_inc" -> "chararray",
    "dubb_esito_inc_ndg" -> "chararray",
    "status_cliente_lab" -> "chararray",
    "tot_util_mortgage" -> "chararray",
    "tot_acco_mortgage" -> "chararray",
    "dt_entrata_default" -> "chararray",
    "cd_stato_def_t0" -> "chararray",
    "cd_tipo_def_t0" -> "chararray",
    "cd_stato_def_a_t12" -> "chararray",
    "cd_rap_ristr" -> "chararray",
    "tp_ristrutt" -> "chararray",
    "tp_cli_scad_scf" -> "chararray",
    "tp_cli_scad_scf_b2" -> "chararray",
    "totale_saldi_0063" -> "chararray",
    "totale_saldi_0260" -> "chararray"
  )

}
