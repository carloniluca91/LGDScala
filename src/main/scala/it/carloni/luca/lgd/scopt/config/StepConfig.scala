package it.carloni.luca.lgd.scopt.config

import it.carloni.luca.lgd.scopt.option.OptionNaming

sealed abstract class StepConfig

case class UfficioConfig(ufficio: String = null) extends StepConfig

case class DtAConfig(dataA: String = null) extends StepConfig

case class DtOsservazioneConfig(dataOsservazione: String = null) extends StepConfig

case class DtAUfficioConfig(ufficio: String = null,
                            dataA: String = null) extends StepConfig {

  override def toString: String =

    s"${OptionNaming.DataAOptionDescription}: $dataA, " +
      s"${OptionNaming.UfficioOptionDescription}: $ufficio"
}

case class DtDaDtAConfig(dataDa: String = null,
                         dataA: String = null) extends StepConfig {

  override def toString: String =

    s"${OptionNaming.DataDaOptionDescription}: $dataDa, " +
      s"${OptionNaming.DataAOptionDescription}: $dataA"
}

case class DtANumeroMesi12Config(dataA: String = null,
                                 numeroMesi1: Int = 0,
                                 numeroMesi2: Int = 0) extends StepConfig
