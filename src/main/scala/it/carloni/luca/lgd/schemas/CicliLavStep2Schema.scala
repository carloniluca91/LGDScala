package it.carloni.luca.lgd.schemas

import scala.collection.immutable.Map

object CicliLavStep2Schema {

  val tlbcidefMax1PigSchema: Map[String, String] = Map(

    "cd_isti" -> "chararray"
    ,"ndg_principale" -> "chararray"
    ,"dt_inizio_ciclo" -> "chararray"
    ,"dt_fine_ciclo" -> "chararray"
    ,"datainiziopd" -> "chararray"
    ,"datainizioristrutt" -> "chararray"
    ,"datainizioinc" -> "chararray"
    ,"datainiziosoff" -> "chararray"
    ,"progr" -> "chararray"
    ,"cd_isti_ced" -> "chararray"
    ,"ndg_ced" -> "chararray"
    ,"dt_riferimento_udct" -> "int"
  )

  val tlbucolLoadPigSchema: Map[String, String] = Map(

    "cd_istituto" -> "chararray"
    ,"ndg" -> "chararray"
    ,"cd_collegamento" -> "chararray"
    ,"ndg_collegato" -> "chararray"
    ,"dt_riferimento" -> "int"
  )

  val tlbudctLoadPigSchema: Map[String, String] = Map(

    "cd_istituto" -> "chararray"
    ,"ndg" -> "chararray"
    ,"dt_riferimento" -> "int"
    ,"REDDITO" -> "chararray"
    ,"TOTALE_UTILIZZI" -> "chararray"
    ,"TOTALE_FIDI_DELIB" -> "chararray"
    ,"TOTALE_ACCORDATO" -> "chararray"
    ,"PATRIMONIO" -> "chararray"
    ,"N_DIPENDENTI" -> "chararray"
    ,"TOT_RISCHI_INDIR" -> "chararray"
    ,"status_ndg" -> "int"
    ,"STATUS_BASILEA2" -> "chararray"
    ,"posiz_soff_inc" -> "int"
    ,"DUBB_ESITO_INC_NDG" -> "chararray"
    ,"STATUS_CLIENTE_LAB" -> "chararray"
    ,"TOT_UTIL_MORTGAGE" -> "chararray"
    ,"TOT_ACCO_MORTGAGE" -> "chararray"
    ,"DT_ENTRATA_DEFAULT" -> "chararray"
    ,"CD_STATO_DEF_T0" -> "chararray"
    ,"CD_TIPO_DEF_T0" -> "chararray"
    ,"CD_STATO_DEF_A_T12" -> "chararray"
    ,"cd_rap_ristr" -> "int"
    ,"TP_RISTRUTT" -> "chararray"
    ,"tp_cli_scad_scf" -> "int"
  )

  val tlburdaLoadPigSchema: Map[String, String] = Map(

    "cd_istituto" -> "chararray"
    ,"ndg" -> "chararray"
    ,"sportello" -> "chararray"
    ,"conto" -> "chararray"
    ,"progr_segmento" -> "int"
    ,"dt_riferimento" -> "int"
    ,"status_rapp" -> "chararray"
    ,"margine_fido_rev" -> "chararray"
    ,"margine_fido_irrev" -> "chararray"
    ,"utilizz_cassa_firm" -> "chararray"
    ,"imp_accordato" -> "chararray"
    ,"flag_ristrutt" -> "chararray"
    ,"imp_accor_ope" -> "chararray"
    ,"imp_accor_non_ope" -> "chararray"
    ,"gg_sconfino" -> "chararray"
    ,"fido_n_ope_sbf_p_i" -> "chararray"
    ,"fido_ope_sbf_p_ill" -> "chararray"
    ,"fido_ope_firma" -> "chararray"
    ,"fido_ope_cassa" -> "chararray"
    ,"fido_non_oper_cas" -> "chararray"
    ,"fido_non_oper_fir" -> "chararray"
    ,"fido_n_ope_sbf_ced" -> "chararray"
    ,"fido_ope_sbf_ced" -> "chararray"
    ,"aut_sconf_cassa" -> "chararray"
    ,"aut_sconf_sbf" -> "chararray"
    ,"aut_sconf_firma" -> "chararray"
    ,"gar_merci" -> "chararray"
    ,"gar_ipoteca" -> "chararray"
    ,"gar_personale" -> "chararray"
    ,"gar_altre" -> "chararray"
    ,"gar_obbl_pr_emiss" -> "chararray"
    ,"gar_obbl_b_centr" -> "chararray"
    ,"gar_obbl_altre" -> "chararray"
    ,"gar_dep_denaro" -> "chararray"
    ,"gar_cd_pr_emiss" -> "chararray"
    ,"gar_altri_valori" -> "chararray"
    ,"valore_dossier" -> "chararray"
    ,"fido_deriv" -> "chararray"
    ,"saldo_cont" -> "chararray"
    ,"saldo_cont_dare" -> "chararray"
    ,"saldo_cont_avere" -> "chararray"
    ,"part_illiq_sbf" -> "chararray"
    ,"ind_util_fido_med" -> "chararray"
    ,"imp_fido_acc_firma" -> "chararray"
    ,"imp_utilizzo_firma" -> "chararray"
    ,"imp_gar_pers_cred" -> "chararray"
    ,"imp_sal_lq_med_dar" -> "chararray"
    ,"imp_utilizzo_cassa" -> "chararray"
  )

  val tlbucocLoadPigSchema: Map[String, String] = Map(

    "cd_istituto" -> "chararray"
    ,"ndg" -> "chararray"
    ,"sportello" -> "chararray"
    ,"conto" -> "chararray"
    ,"progr_segmento" -> "int"
    ,"dt_riferimento" -> "int"
    ,"fido_non_oper_cas" -> "chararray"
    ,"fido_ope_cassa" -> "chararray"
    ,"fido_op_pf_ns_mut" -> "chararray"
    ,"fido_ope_sbf" -> "chararray"
    ,"mass_util_sbf" -> "chararray"
    ,"sal_con_ced_ef_sbf" -> "chararray"
    ,"saldo_cont_dare" -> "chararray"
    ,"saldo_cont_avere" -> "chararray"
    ,"part_illiq_sbf" -> "chararray"
    ,"assegni_pr_ins_on" -> "chararray"
    ,"ass_trz_ins_ng_sbf" -> "chararray"
    ,"addebiti_in_sosp" -> "chararray"
  )
}
