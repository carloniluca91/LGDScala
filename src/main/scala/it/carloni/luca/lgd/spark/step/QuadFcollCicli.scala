package it.carloni.luca.lgd.spark.step

import it.carloni.luca.lgd.schema.QuadFcollCicliSchema
import it.carloni.luca.lgd.scopt.config.UfficioConfig
import it.carloni.luca.lgd.spark.common.AbstractSparkStep
import org.apache.spark.sql.functions.lit
import org.apache.log4j.Logger

import scala.collection.mutable

class QuadFcollCicli extends AbstractSparkStep[UfficioConfig] {

  private val logger = Logger.getLogger(getClass)

  // STEP PATHS
  private val fcollCsvPath = getPropertyValue("quad.fcoll.cicli.fcoll.csv")
  private val cicliNdgLoadCsvPath = getPropertyValue("quad.fcoll.cicli.cicli.ndg.load.csv")
  private val quadFcollCicliOutputPath = getPropertyValue("quad.fcoll.cicli.file.out")

  // STEP SCHEMAS
  private val fcollPigSchema: mutable.LinkedHashMap[String, String] = QuadFcollCicliSchema.fcollPigSchema
  private val cicliNdgLoadPigSchema: mutable.LinkedHashMap[String, String] = QuadFcollCicliSchema.cicliNdgLoadPigSchema

  override def run(ufficioConfig: UfficioConfig): Unit = {

    val ufficio = ufficioConfig.ufficio

    logger.info(ufficioConfig)

    logger.info(s"quad.fcoll.cicli.fcoll.csv: $fcollCsvPath")
    logger.info(s"quad.fcoll.cicli.cicli.ndg.load.csv: $cicliNdgLoadCsvPath")
    logger.info(s"quad.fcoll.cicli.file.out: $quadFcollCicliOutputPath")

    val fcoll = readCsvFromPathUsingSchema(fcollCsvPath, fcollPigSchema)
    val cicliNdgLoad = readCsvFromPathUsingSchema(cicliNdgLoadCsvPath, cicliNdgLoadPigSchema)

    // JOIN cicli_ndg_load BY (cd_isti_coll, ndg_coll) FULL OUTER, fcoll BY (istituto_collegato, ndg_collegato);
    val fileOutJoinCondition = (cicliNdgLoad("cd_isti_coll") === fcoll("istituto_collegato")) &&
      (cicliNdgLoad("ndg_coll") === fcoll("ndg_collegato"))

    val fileOut = cicliNdgLoad.join(fcoll, fileOutJoinCondition, "full_outer")
      .select(lit(ufficio).as("ufficio"), cicliNdgLoad("cd_isti"), cicliNdgLoad("ndg_principale"),
        cicliNdgLoad("dt_inizio_ciclo"), cicliNdgLoad("cd_isti_coll"), cicliNdgLoad("ndg_coll"),
        fcoll("codicebanca").as("fcoll_codicebanca"), fcoll("ndgprincipale").as("fcoll_ndgprincipale"),
        fcoll("istituto_collegato").as("fcoll_cd_istituto_coll"), fcoll("ndg_collegato").as("fcoll_ndg_coll"),
        fcoll("datainiziodef").as("fcoll_data_inizio_def"))
      .distinct()

    writeDataFrameAsCsvToPath(fileOut, quadFcollCicliOutputPath)
  }
}

