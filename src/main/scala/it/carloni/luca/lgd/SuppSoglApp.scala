package it.carloni.luca.lgd

import it.carloni.luca.lgd.options.OptionFactory
import it.carloni.luca.lgd.params.LgdParamsParser
import it.carloni.luca.lgd.steps.SuppSogl
import org.apache.commons.cli.Option

object SuppSoglApp extends App {

  // REQUIRED PARAMETERS : $numero_mesi_1, $numero_mesi_2, $data_a

  val numeroMesi1Option: Option = OptionFactory.getNumeroMesi1Option
  val numeroMesi2Option: Option = OptionFactory.getNumeroMesi2Option
  val dataAOption: Option = OptionFactory.getDataAOpton
  val suppSoglOptionList = List(numeroMesi1Option, numeroMesi2Option, dataAOption)

  val lgdParamsParser = new LgdParamsParser(args, suppSoglOptionList)
  val suppSogl = new SuppSogl(lgdParamsParser.getStepParamsMap)
  suppSogl.run()

}
