package it.carloni.luca.lgd.spark.step

import it.carloni.luca.lgd.schema.QuadFrappSchema
import it.carloni.luca.lgd.scopt.config.UfficioConfig
import it.carloni.luca.lgd.spark.common.AbstractSparkStep
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit
import org.apache.log4j.Logger

import scala.collection.mutable

class QuadFrapp extends AbstractSparkStep[UfficioConfig] {

  private val logger = Logger.getLogger(getClass)

  // STEP PATHS
  private val fcollCsvPath = getPropertyValue("quad.frapp.fcoll.csv")
  private val hadoopFrappCsvPath = getPropertyValue("quad.frapp.hadoop.frapp.csv")
  private val oldFrappLoadCsvPath = getPropertyValue("quad.frapp.old.frapp.load.csv")
  private val hadoopFrappOutputPath = getPropertyValue("quad.frapp.hadoop.frapp.out")
  private val oldFrappOutputPath = getPropertyValue("quad.frapp.old.frapp.out")

  // STEP SCHEMAS
  private val fcollPigSchema: mutable.LinkedHashMap[String, String] = QuadFrappSchema.fcollPigSchema
  private val hadoopFrappPigSchema: mutable.LinkedHashMap[String, String] = QuadFrappSchema.hadoopFrappPigSchema
  private val oldFrappLoadPigSchema: mutable.LinkedHashMap[String, String] = QuadFrappSchema.oldFrappLoadPigSchema

  override def run(ufficioConfig: UfficioConfig): Unit = {

    val ufficio: String = ufficioConfig.ufficio

    logger.info(ufficioConfig)

    logger.info(s"quad.frapp.fcoll.csv: $fcollCsvPath")
    logger.info(s"quad.frapp.hadoop.frapp.csv: $hadoopFrappCsvPath")
    logger.info(s"quad.frapp.old.frapp.load.csv: $oldFrappLoadCsvPath")
    logger.info(s"quad.frapp.hadoop.frapp.out: $hadoopFrappOutputPath")
    logger.info(s"quad.frapp.old.frapp.out: $oldFrappOutputPath")

    val hadoopFrapp = readCsvFromPathUsingSchema(hadoopFrappCsvPath, hadoopFrappPigSchema)
    val oldFrappLoad = readCsvFromPathUsingSchema(oldFrappLoadCsvPath, oldFrappLoadPigSchema)
    val fcoll = readCsvFromPathUsingSchema(fcollCsvPath, fcollPigSchema)

    // 191

    // JOIN oldfrapp_load BY (CODICEBANCA, NDG), fcoll BY (ISTITUTO_COLLEGATO, NDG_COLLEGATO);
    val oldFrappJoinCondition = oldFrappLoad("CODICEBANCA") === fcoll("ISTITUTO_COLLEGATO") &&
      oldFrappLoad("NDG") === fcoll("NDG_COLLEGATO")

    // FILTER oldfrapp_load_fcoll_join
    // BY ToDate(oldfrapp_load::DT_RIFERIMENTO,'yyyyMMdd') >= ToDate( fcoll::DATAINIZIODEF,'yyyyMMdd')
    // AND ToDate(oldfrapp_load::DT_RIFERIMENTO,'yyyyMMdd') <= ToDate( fcoll::DATAFINEDEF,'yyyyMMdd'  )

    val oldFrappFilterCondition = oldFrappLoad("DT_RIFERIMENTO").between(fcoll("DATAINIZIODEF"), fcoll("DATAFINEDEF"))
    val oldFrapp = oldFrappLoad.join(fcoll, oldFrappJoinCondition)
      .filter(oldFrappFilterCondition)
      .select(oldFrappLoad("*"), fcoll("CODICEBANCA").as("CODICEBANCA_PRINC"), fcoll("NDGPRINCIPALE"), fcoll("DATAINIZIODEF"))

    // 278

    // JOIN hadoop_frapp BY (codicebanca_princ, ndgprincipale, datainiziodef, codicebanca, ndg, sportello, conto, datariferimento) FULL OUTER,
    // oldfrapp BY (CODICEBANCA_PRINC, NDGPRINCIPALE, DATAINIZIODEF, CODICEBANCA, NDG, SPORTELLO, CONTO, DT_RIFERIMENTO);

    val hadoopFrappOldFrappJoinCondition: Column = Seq("codicebanca_princ", "ndgprincipale", "datainiziodef", "codicebanca", "ndg", "sportello", "conto")
      .map((columnName: String) => hadoopFrapp(columnName) === oldFrapp(columnName.toUpperCase))
      .reduce(_ && _) && hadoopFrapp("datariferimento") === oldFrapp("DT_RIFERIMENTO")

    val hadoopFrappOldFrappJoin = hadoopFrapp.join(oldFrapp, hadoopFrappOldFrappJoinCondition, "full_outer")

    // FILTER hadoop_frapp_oldfrapp_join BY oldfrapp::CODICEBANCA IS NULL;
    val hadoopFrappOut = hadoopFrappOldFrappJoin
      .filter(oldFrapp("CODICEBANCA").isNull)
      .select(lit(ufficio).as("ufficio"), hadoopFrapp("*"), oldFrapp("*"))

    // FILTER hadoop_frapp_oldfrapp_join BY hadoop_frapp::codicebanca IS NULL;
    val oldFrappOut = hadoopFrappOldFrappJoin
      .filter(hadoopFrapp("codicebanca").isNull)
      .select(lit(ufficio).as("ufficio"), hadoopFrapp("*"), oldFrapp("*"))

    // 609

    writeDataFrameAsCsvToPath(hadoopFrappOut, hadoopFrappOutputPath)
    writeDataFrameAsCsvToPath(oldFrappOut, oldFrappOutputPath)
  }
}
