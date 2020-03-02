package it.carloni.luca.lgd

import it.carloni.luca.lgd.options.OptionFactory
import it.carloni.luca.lgd.params.LgdParamsParser
import it.carloni.luca.lgd.steps.CicliLavStep2
import org.apache.commons.cli.Option

object CicliLavStep2App extends App {

  // REQUESTED PARAMS: $periodo

  val periodoOption: Option = OptionFactory.getPeriodoOption
  val cicliLavStep2OptionList = List(periodoOption)

  val lgdParamParser = new LgdParamsParser(args, cicliLavStep2OptionList)
  val cicliLavStep2 = new CicliLavStep2(lgdParamParser.getStepParamsMap)
  cicliLavStep2.run()

}
