package it.carloni.luca.lgd

import it.carloni.luca.lgd.options.OptionFactory
import it.carloni.luca.lgd.params.LgdParamsParser
import it.carloni.luca.lgd.steps.FposiMonthly
import org.apache.commons.cli.Option

object FposiMonthlyApp extends App {

  // REQUESTED PARAMS: $periodo, $data_a

  val periodoOption: Option = OptionFactory.getPeriodoOption
  val dataAOption: Option = OptionFactory.getDataAOpton
  val fposiMonthlyOptionList = List(periodoOption, dataAOption)

  val lgdParamsParser = new LgdParamsParser(args, fposiMonthlyOptionList)
  val fposiMonthly = new FposiMonthly(lgdParamsParser.getStepParamsMap)
  fposiMonthly.run()

}
