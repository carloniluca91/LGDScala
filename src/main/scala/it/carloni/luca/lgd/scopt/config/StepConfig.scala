package it.carloni.luca.lgd.scopt.config

import it.carloni.luca.lgd.scopt.option.OptionNaming

sealed abstract class StepConfig

class NoValueConfig extends StepConfig

case class UfficioConfig(ufficio: String = null) extends StepConfig

case class DtAConfig(dataA: String = null) extends StepConfig {

  override def toString: String = {

    val dataADescription: String = OptionNaming.DataAOptionDescription
    s"$dataADescription: $dataA"
  }
}

case class DtOsservazioneConfig(dataOsservazione: String = null) extends StepConfig {

  override def toString: String = {

    val dataOsservazioneDescription: String = OptionNaming.DataOsservazioneOptionDescription
    s"$dataOsservazioneDescription: $dataOsservazione"
  }
}

case class DtAUfficioConfig(ufficio: String = null,
                            dataA: String = null) extends StepConfig {

  override def toString: String = {

    val dataDescription = OptionNaming.DataAOptionDescription
    val ufficioDescription = OptionNaming.UfficioOptionDescription
    s"$dataDescription: $dataA, $ufficioDescription: $ufficio"
  }
}

case class DtDaDtAConfig(dataDa: String = null,
                         dataA: String = null) extends StepConfig {

  override def toString: String = {

    val dataDaDescription = OptionNaming.DataDaOptionDescription
    val dataADescription = OptionNaming.DataAOptionDescription
    s"$dataDaDescription: $dataDa, $dataADescription: $dataA"
  }
}

case class DtANumeroMesi12Config(dataA: String = null,
                                 numeroMesi1: Int = 0,
                                 numeroMesi2: Int = 0) extends StepConfig {

  override def toString: String = {

    val dataADescription = OptionNaming.DataAOptionDescription
    val numeroMesi1Description = OptionNaming.NumeroMesi1Description
    val numeroMesi2Description = OptionNaming.NumeroMesi2Description
    s"$dataADescription: $dataA, $numeroMesi1Description: $numeroMesi1, $numeroMesi2Description: $numeroMesi2"
  }
}
