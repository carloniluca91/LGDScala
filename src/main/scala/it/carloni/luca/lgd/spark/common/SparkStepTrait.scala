package it.carloni.luca.lgd.spark.common

import it.carloni.luca.lgd.scopt.config.StepConfig

trait SparkStepTrait[T <: StepConfig] {

  def run(t: T): Unit

}
