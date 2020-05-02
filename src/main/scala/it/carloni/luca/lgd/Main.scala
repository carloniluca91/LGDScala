package it.carloni.luca.lgd

import it.carloni.luca.lgd.scopt.parser.StepParser

object Main extends App {

  val stepNameConfig = StepParser.StepNameConfig
  val stepNameOptionParser = StepParser.stepNameOptionParser

  stepNameOptionParser.parse(args, stepNameConfig()) match {

    case Some(value) =>

      new StepRunner().run(args, value.stepName)

    case None => // arguments are bad, error message will have been displayed
  }
}
