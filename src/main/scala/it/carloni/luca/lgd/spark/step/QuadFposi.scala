package it.carloni.luca.lgd.spark.step

import it.carloni.luca.lgd.schema.QuadFposiSchema
import it.carloni.luca.lgd.scopt.config.UfficioConfig
import it.carloni.luca.lgd.spark.common.{AbstractSparkStep, SparkEnums}
import org.apache.spark.sql.functions.{callUDF, col, lit}
import org.apache.log4j.Logger
import org.apache.spark.sql.Column

import scala.collection.mutable

class QuadFposi extends AbstractSparkStep[UfficioConfig] {

  private val logger = Logger.getLogger(getClass)

  // STEP PATHS
  private val hadoopFposiCsvPath = getPropertyValue("quad.fposi.hadoop.fposi.csv")
  private val oldFposiLoadCsvPath = getPropertyValue("quad.fposi.old.fposi.load.csv")
  private val hadoopFposiOutputPath = getPropertyValue("quad.fposi.hadoop.fposi.out")
  private val oldFposiOutputPath = getPropertyValue("quad.fposi.old.fposi.out")
  private val abbinatiOutputPath = getPropertyValue("quad.fposi.abbinati.out")

  // STEP SCHEMAS
  private val hadoopFposiPigSchema: mutable.LinkedHashMap[String, String] = QuadFposiSchema.hadoopFposiPigSchema
  private val oldFposiLoadPigSchema: mutable.LinkedHashMap[String, String] = QuadFposiSchema.oldFposiLoadPigSchema

  override def run(ufficioConfig: UfficioConfig): Unit = {

    val ufficio: String = ufficioConfig.ufficio

    logger.info(s"quad.fposi.hadoop.fposi.csv: $hadoopFposiCsvPath")
    logger.info(s"quad.fposi.old.fposi.load.csv: $oldFposiLoadCsvPath")
    logger.info(s"quad.fposi.hadoop.fposi.out: $hadoopFposiOutputPath")
    logger.info(s"quad.fposi.old.fposi.out: $oldFposiOutputPath")
    logger.info(s"quad.fposi.abbinati.out: $abbinatiOutputPath")

    val hadoopFposi = readCsvFromPathUsingSchema(hadoopFposiCsvPath, hadoopFposiPigSchema)

    // 45

    /*
    		 ToString(ToDate( datainizioDEF,'yy-MM-dd'),'yyyyMMdd')   as DATAINIZIODEF
				,ToString(ToDate( dataFINEDEF,'yy-MM-dd'),'yyyyMMdd')     as DATAFINEDEF
				,ToString(ToDate( dataINIZIOPD,'yy-MM-dd'),'yyyyMMdd')    as DATAINIZIOPD
				,ToString(ToDate( datainizioinc,'yy-MM-dd'),'yyyyMMdd')   as DATAINIZIOINC
				,ToString(ToDate( dataSOFFERENZA,'yy-MM-dd'),'yyyyMMdd')  as DATASOFFERENZA
				,codicebanca     as CODICEBANCA
				,ndgprincipale   as NDGPRINCIPALE
				,flagincristrut  as FLAGINCRISTRUT
				,cumulo          as CUMULO
     */

    val Y2_M2_D2Format = "yy-MM-dd"
    val Y4M2D2Format = SparkEnums.DateFormats.Y4M2D2Format.toString
    val oldFposiGen = readCsvFromPathUsingSchema(oldFposiLoadCsvPath, oldFposiLoadPigSchema)
      .select(changeDateFormatFromY2toY4(col("datainizioDEF"), Y2_M2_D2Format, Y4M2D2Format).as("DATAINIZIODEF"),
        changeDateFormatFromY2toY4(col("dataFINEDEF"), Y2_M2_D2Format, Y4M2D2Format).as("DATAFINEDEF"),
        changeDateFormatFromY2toY4(col("dataINIZIOPD"), Y2_M2_D2Format, Y4M2D2Format).as("DATAINIZIOPD"),
        changeDateFormatFromY2toY4(col("datainizioinc"), Y2_M2_D2Format, Y4M2D2Format).as("DATAINIZIOINC"),
        changeDateFormatFromY2toY4(col("dataSOFFERENZA"), Y2_M2_D2Format, Y4M2D2Format).as("DATASOFFERENZA"),
        col("codicebanca").as("CODICEBANCA"), col("ndgprincipale").as("NDGPRINCIPALE"),
        col("flagincristrut").as("FLAGINCRISTRUT"), col("cumulo").as("CUMULO"))

    val oldFposi = oldFposiGen
      .filter(col("DATAINIZIODEF") between("20070131", "20071231"))

    // 85

    // JOIN hadoop_fposi BY (codicebanca, ndgprincipale, datainiziodef) FULL OUTER, oldfposi BY (CODICEBANCA, NDGPRINCIPALE, DATAINIZIODEF);
    val hadoopFposiOldfposiJoinCondition: Column = Seq("codicebanca", "ndgprincipale", "datainiziodef")
      .map((columnName: String) => hadoopFposi(columnName) === oldFposi(columnName.toUpperCase))
      .reduce(_ && _)

    val columnsToSelect: Seq[Column] = Seq(lit(ufficio).as("ufficio"), hadoopFposi("codicebanca"), hadoopFposi("ndgprincipale"),
      hadoopFposi("datainiziodef"), hadoopFposi("datafinedef"), hadoopFposi("ndg_gruppo"), hadoopFposi("datainiziopd"), hadoopFposi("datainizioinc"),
      hadoopFposi("datainizioristrutt"), hadoopFposi("datainiziosoff"), hadoopFposi("totaccordatodatdef"), hadoopFposi("totutilizzdatdef"),
      hadoopFposi("naturagiuridica_segm"), hadoopFposi("intestazione"), hadoopFposi("codicefiscale_segm"), hadoopFposi("partitaiva_segm"),
      hadoopFposi("sae_segm"), hadoopFposi("rae_segm"), hadoopFposi("ciae_ndg"), hadoopFposi("provincia_segm"), hadoopFposi("ateco"),
      hadoopFposi("segmento"), hadoopFposi("databilseg"), hadoopFposi("strbilseg"), hadoopFposi("attivobilseg"), hadoopFposi("fatturbilseg"),
      oldFposi("DATAINIZIODEF"), oldFposi("DATAFINEDEF"), oldFposi("DATAINIZIOPD"), oldFposi("DATAINIZIOINC"), oldFposi("DATASOFFERENZA"),
      oldFposi("CODICEBANCA"), oldFposi("NDGPRINCIPALE"), oldFposi("FLAGINCRISTRUT"), oldFposi("CUMULO"))

    val hadoopFposiOldfposiJoin = hadoopFposi.join(oldFposi, hadoopFposiOldfposiJoinCondition, "full_outer")

    val hadoopFposiOut = hadoopFposiOldfposiJoin
      .filter(oldFposi("CODICEBANCA").isNull)
      .select(columnsToSelect: _*)

    // 145

    val oldFposiOut = hadoopFposiOldfposiJoin
      .filter(hadoopFposi("codicebanca").isNull)
      .select(columnsToSelect: _*)

    // 184

    /*
      FILTER hadoop_fposi_oldfposi_join
      BY hadoop_fposi::codicebanca IS NOT NULL
      AND oldfposi::CODICEBANCA IS NOT NULL
      AND hadoop_fposi::datafinedef == '99991231'
      AND hadoop_fposi::datainiziopd       != oldfposi::DATAINIZIOPD
      AND hadoop_fposi::datainizioinc      != oldfposi::DATAINIZIOINC
      AND hadoop_fposi::datainiziosoff     != oldfposi::DATASOFFERENZA
     */

    val abbinatiOutFilterCondition = hadoopFposi("codicebanca").isNotNull &&
      oldFposi("CODICEBANCA").isNotNull &&
      hadoopFposi("datafinedef") === "99991231" &&
      hadoopFposi("datainiziopd") =!= oldFposi("DATAINIZIOPD") &&
      hadoopFposi("datainizioinc") =!= oldFposi("DATAINIZIOINC") &&
      hadoopFposi("datainiziosoff") =!= oldFposi("DATASOFFERENZA")

    val abbinatiOut = hadoopFposiOldfposiJoin
      .filter(abbinatiOutFilterCondition)
      .select(columnsToSelect: _*)

    // 223

    writeDataFrameAsCsvToPath(hadoopFposiOut, hadoopFposiOutputPath)
    writeDataFrameAsCsvToPath(oldFposiOut, oldFposiOutputPath)
    writeDataFrameAsCsvToPath(abbinatiOut, abbinatiOutputPath)

  }

  private def changeDateFormatFromY2toY4(column: Column, oldFormat: String, newFormat: String): Column =

    callUDF(SparkEnums.UDFsNames.ChangeDateFormatFromY2toY4UDFName.toString,
      column,
      lit(oldFormat),
      lit(newFormat))
}
