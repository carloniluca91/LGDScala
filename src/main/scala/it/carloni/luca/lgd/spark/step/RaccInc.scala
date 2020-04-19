package it.carloni.luca.lgd.spark.step

import it.carloni.luca.lgd.commons.LGDCommons
import it.carloni.luca.lgd.schema.RaccIncSchema
import it.carloni.luca.lgd.spark.common.AbstractSparkStep
import it.carloni.luca.lgd.spark.utils.SparkUtils.addDurationUDF
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DataTypes
import org.apache.log4j.Logger

class RaccInc extends AbstractSparkStep {

  private val logger = Logger.getLogger(getClass)

  // STEP PATHS
  private val tlbmignCsvPath = getPropertyValue("racc.inc.tlbmign.path.csv")
  private val raccIncOutputPath = getPropertyValue("racc.inc.racc.inc.out")

  // STEP SCHEMAS
  private val tlbmignPigSchema = RaccIncSchema.tlbmignPigSchema

  override def run(): Unit = {

    logger.debug(s"tlbmignCsvPath: $tlbmignCsvPath")
    logger.debug(s"raccIncOutputPath: $raccIncOutputPath")

    val Y4M2D2Format = LGDCommons.DatePatterns.Y4M2D2Pattern
    val raccInc = readCsvFromPathUsingSchema(tlbmignCsvPath, tlbmignPigSchema)
      .select(col("cd_isti_ric").as("ist_ric_inc"),
        col("ndg_ric").as("ndg_ric_inc"),
        lit(null).cast(DataTypes.StringType).as("num_ric_inc"),
        col("cd_isti_ced").as("ist_ced_inc"),
        col("ndg_ced").as("ndg_ced_inc"),
        lit(null).cast(DataTypes.StringType).as("num_ced_inc"),
        addDurationUDF(col("data_migraz"), Y4M2D2Format, 1).as("data_fine_primo_mese_ric"))

    writeDataFrameAsCsvToPath(raccInc, raccIncOutputPath)
  }
}
