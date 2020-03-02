package it.carloni.luca.lgd.schemas

import scala.collection.immutable.Map

object FposiMonthlySchema {

  val tlbcidef1PigSchema: Map[String, String] = Map(

    "codicebanca" -> "chararray"
    ,"ndgprincipale" -> "chararray"
    ,"datainiziodef" -> "int"
    ,"datafinedef" -> "chararray"
    ,"datainiziopd" -> "chararray"
    ,"datainizioristrutt" -> "chararray"
    ,"datainizioinc" -> "chararray"
    ,"datainiziosoff" -> "chararray"
    ,"c_key" -> "chararray"
    ,"tipo_segmne" -> "chararray"
    ,"sae_segm" -> "chararray"
    ,"rae_segm" -> "chararray"
    ,"segmento" -> "chararray"
    ,"tp_ndg" -> "chararray"
    ,"provincia_segm" -> "chararray"
    ,"databilseg" -> "chararray"
    ,"strbilseg" -> "chararray"
    ,"attivobilseg" -> "chararray"
    ,"fatturbilseg" -> "chararray"
    ,"ndg_collegato" -> "chararray"
    ,"codicebanca_collegato" -> "chararray"
    ,"cd_collegamento" -> "chararray"
    ,"cd_fiscale" -> "chararray"
    ,"dt_rif_udct" -> "int"
    ,"util_cassa" -> "double"
    ,"imp_accordato" -> "double"
  )

  val ciclilavListadefPigSchema: Map[String, String] = Map(

    "cd_isti" -> "chararray"
    ,"ndg_principale" -> "chararray"
    ,"dt_inizio_ciclo" -> "int"
    ,"cd_isti_coll" -> "chararray"
  )

  val tlbungrPigSchema: Map[String, String] = Map(

    "dt_riferimento" -> "int"
    ,"cd_istituto" -> "chararray"
    ,"ndg" -> "chararray"
    ,"ndg_gruppo" -> "chararray"
  )

  val tlbuactLoadPigSchema: Map[String, String] = Map(

    "cd_istituto" -> "chararray"
    ,"ndg" -> "chararray"
    ,"dt_riferimento" -> "int"
    ,"cab_prov_com_stato" -> "chararray"
    ,"provincia" -> "chararray"
    ,"sae" -> "chararray"
    ,"rae" -> "chararray"
    ,"ciae" -> "chararray"
    ,"sportello" -> "chararray"
    ,"area_affari" -> "chararray"
    ,"tp_ndg" -> "chararray"
    ,"intestazione" -> "chararray"
    ,"partita_iva" -> "chararray"
    ,"cd_fiscale" -> "chararray"
    ,"ctp_cred_proc_conc" -> "chararray"
    ,"cliente_protestato" -> "chararray"
    ,"status_sygei" -> "chararray"
    ,"ndg_caponucleo" -> "chararray"
    ,"specie_giuridica" -> "chararray"
    ,"dt_inizio_rischio" -> "chararray"
    ,"dt_revoca_fidi" -> "chararray"
    ,"dt_cens_anagrafe" -> "chararray"
    ,"cab_luogo_nascita" -> "chararray"
    ,"dt_costituz_nascit" -> "chararray"
    ,"tp_contr_basilea1" -> "chararray"
    ,"flag_fallibile" -> "chararray"
    ,"stato_controparte" -> "chararray"
    ,"dt_estinz_ctp" -> "chararray"
    ,"cliente_fido_rev" -> "chararray"
    ,"tp_controparte" -> "chararray"
    ,"grande_sett_attiv" -> "chararray"
    ,"grande_ramo_attiv" -> "chararray"
    ,"n058_interm_vigil" -> "chararray"
    ,"n160_cod_ateco" -> "chararray"
    )
}
