package it.carloni.luca.lgd.step

import it.carloni.luca.lgd.schema.RaccSoffSchema
import it.carloni.luca.lgd.spark.AbstractSparkStep
import org.apache.log4j.Logger

class RaccSoff extends AbstractSparkStep {

  private val logger = Logger.getLogger(getClass)

  // STEP PATHS
  private val dblabtlbxd9CsvPath = getPropertyValue("racc.soff.dblabtlbxd9.path.csv")
  private val dllabOutputPath = getPropertyValue("racc.soff.dllab")

  // STEP SCHEMAS
  private val dblabtlbxd9PigSchema = RaccSoffSchema.dblabtlbxd9PigSchema

  override def run(): Unit = {

    logger.debug(s"dblabtlbxd9CsvPath: $dblabtlbxd9CsvPath")
    logger.debug(s"dllabOutputPath: $dllabOutputPath")

    val raccSoffOut = readCsvFromPathUsingSchema(dblabtlbxd9CsvPath, dblabtlbxd9PigSchema)
      .withColumnRenamed("istricsof", "IST_RIC_SOF")
      .withColumnRenamed("ndgricsof", "NDG_RIC_SOF")
      .withColumnRenamed("numricsof", "NUM_RIC_SOF")
      .withColumnRenamed("istcedsof", "IST_CED_SOF")
      .withColumnRenamed("ndgcedsof", "NDG_CED_SOF")
      .withColumnRenamed("numcedsof", "NUM_CED_SOF")
      .withColumnRenamed("data_primo_fine_me", "DATA_FINE_PRIMO_MESE_RIC")

    writeDataFrameAsCsvToPath(raccSoffOut, dllabOutputPath)
  }
}
