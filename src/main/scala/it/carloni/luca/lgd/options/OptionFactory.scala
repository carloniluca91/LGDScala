package it.carloni.luca.lgd.options

import org.apache.commons.cli.Option

object OptionFactory {

  private def buildOptionWithArg(shortOption: String, longOption: String, optionDescription: String): Option =
    Option.builder(shortOption)
      .longOpt(longOption)
      .desc(optionDescription)
      .hasArg
      .required
      .build

  def getDataDaOption: Option = buildOptionWithArg(OptionNaming.DataDaShortOption,
    OptionNaming.DataDaLongOption, OptionNaming.DataDaOptionDescription)

  def getDataAOpton: Option = buildOptionWithArg(OptionNaming.DataAShortOption,
    OptionNaming.DataALongOption, OptionNaming.DataAOptionDescription)

  def getDataOsservazioneOption: Option = buildOptionWithArg(OptionNaming.DataOsservazioneShortOption,
    OptionNaming.DataOsservazioneLongOption, OptionNaming.DataDaOptionDescription)

  def getUfficioOption: Option = buildOptionWithArg(OptionNaming.UfficioShortOption,
    OptionNaming.UfficioLongOption, OptionNaming.UfficioOptionDescription)

  def getNumeroMesi1Option: Option = buildOptionWithArg(OptionNaming.NumeroMesi1ShortOption,
    OptionNaming.NumeroMesi1LongOption, OptionNaming.NumeroMesi1Description)

  def getNumeroMesi2Option: Option = buildOptionWithArg(OptionNaming.NumeroMesi2ShortOption,
    OptionNaming.NumeroMesi2LongOption, OptionNaming.NumeroMesi2Description)

  def getPeriodoOption: Option = buildOptionWithArg(OptionNaming.PeriodoShortOption,
    OptionNaming.PeriodoLongOption, OptionNaming.PeriodoOptionDescription)

}
