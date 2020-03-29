package it.carloni.luca.lgd.step

import it.carloni.luca.lgd.common.AbstractStep
import it.carloni.luca.lgd.schema.FpasperdSchema
import it.carloni.luca.lgd.common.utils.SparkUtils.{addDuration, daysBetween, toIntType}
import org.apache.spark.sql.functions.{col, first, lit, substring}
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DataTypes
import org.apache.log4j.Logger

class Fpasperd extends AbstractStep {

  private val logger = Logger.getLogger(getClass)

  // STEP PATHS
  private val cicliNdgCsvPath = getPropertyValue("fpasperd.cicli.ndg.path.csv")
  private val tlbpaspeFilterCsvPath = getPropertyValue("fpasperd.tlbpaspe.filter.csv")
  private val tlbpaspeossCsvPath = getPropertyValue("fpasperd.tlbpaspeoss.csv")
  private val paspePaspeossGenDistOutputPath = getPropertyValue("fpasperd.paspe.paspeoss.gen.dist")

  // STEP SCHEMAS
  private val tlbcidefLoadPigSchema = FpasperdSchema.tlbcidefLoadPigSchema
  private val tlbpaspeFilterPigSchema = FpasperdSchema.tlbpaspeFilterPigSchema

  override def run(): Unit = {

    logger.debug(s"cicliNdgCsvPath: $cicliNdgCsvPath")
    logger.debug(s"tlbpaspeFilterCsvPath: $tlbpaspeFilterCsvPath")
    logger.debug(s"tlbpaspeossCsvPath: $tlbpaspeossCsvPath")
    logger.debug(s"paspePaspeossGenDistOutputPath: $paspePaspeossGenDistOutputPath")

    val Y4M2D2Format = "yyyyMMdd"

    // 19
    // ,(int)ToString(AddDuration( ToDate( (chararray)datafinedef,'yyyyMMdd' ),'P2M' ),'yyyyMMdd' )	AS  datafinedef
    val dataFineDefCol = toIntType(addDuration(col("datafinedef"), Y4M2D2Format, 2)).as("datafinedef")

    val tlbcidef = readCsvFromPathUsingSchema(cicliNdgCsvPath, tlbcidefLoadPigSchema)
      .select(col("codicebanca"), col("ndgprincipale"), col("datainiziodef"),
        dataFineDefCol, col("codicebanca_collegato"), col("ndg_collegato"))

    // 56

    val tlbpaspeFilter = readCsvFromPathUsingSchema(tlbpaspeFilterCsvPath, tlbpaspeFilterPigSchema)

    // 71

    // JOIN tlbpaspe_filter BY (cd_istituto, ndg) LEFT, tlbcidef BY (codicebanca_collegato, ndg_collegato);
    val tlbcidefTlbpaspeFilterJoinConditionCol = (tlbpaspeFilter("cd_istituto") === tlbcidef("codicebanca_collegato")) &&
      (tlbpaspeFilter("ndg") === tlbcidef("ndg_collegato"))

    val tlbcidefTlbpaspeFilterJoin = tlbpaspeFilter.join(tlbcidef, tlbcidefTlbpaspeFilterJoinConditionCol, "left")

    // DaysBetween( ToDate((chararray)tlbcidef::datafinedef,'yyyyMMdd' ), ToDate((chararray)tlbpaspe_filter::datacont,'yyyyMMdd' ) ) as days_diff
    val daysBetweenCol = daysBetween(tlbcidef("datafinedef"), tlbpaspeFilter("datacont"), Y4M2D2Format).as("days_diff")

    // FILTER BY (int)SUBSTRING((chararray)tlbpaspe_filter::datacont,0,6) >= (int)SUBSTRING((chararray)tlbcidef::datainiziodef,0,6)
    // AND (int)SUBSTRING((chararray)tlbpaspe_filter::datacont,0,6) < (int)SUBSTRING( (chararray)tlbcidef::datafinedef,0,6 )

    val fpasperdBetweenGen = tlbcidefTlbpaspeFilterJoin
      .filter((substringAndToInt(tlbpaspeFilter("datacont"), 0, 6) >= substringAndToInt(tlbcidef("datainiziodef"), 0, 6)) &&
        (substringAndToInt(tlbpaspeFilter("datacont"), 0, 6) < substringAndToInt(tlbcidef("datafinedef"), 0, 6)))
      .select(tlbpaspeFilter("cd_istituto"), tlbpaspeFilter("ndg"), tlbpaspeFilter("datacont"), tlbpaspeFilter("causale"),
        tlbpaspeFilter("importo"), tlbcidef("codicebanca"), tlbcidef("ndgprincipale"), tlbcidef("datainiziodef"), tlbcidef("datafinedef"),
        daysBetweenCol)

    // 104

    // GROUP fpasperd_between_gen BY ( cd_istituto, ndg, datacont, causale, codicebanca, ndgprincipale );
    // ORDER fpasperd_between_gen by days_diff ASC;

    val fpasperdBetweenOutWindowSpec = Window
      .partitionBy("cd_istituto", "ndg", "datacont", "causale", "codicebanca", "ndgprincipale")
      .orderBy("days_diff")

    val fpasperdBetweenOut = fpasperdBetweenGen
      .select(col("cd_istituto"), col("ndg"), col("datacont"), col("causale"),
        col("importo"), col("codicebanca"), col("ndgprincipale"), col("datainiziodef"))
      .withColumn("importo", first(col("importo").over(fpasperdBetweenOutWindowSpec)))
      .withColumn("datainiziodef", first(col("datainiziodef").over(fpasperdBetweenOutWindowSpec)))

    // 127

    val fpasperdOtherGen = tlbcidefTlbpaspeFilterJoin
      .filter(tlbcidef("codicebanca").isNotNull)
      .select(tlbpaspeFilter("cd_istituto"), tlbpaspeFilter("ndg"), tlbpaspeFilter("datacont"), tlbpaspeFilter("causale"), tlbpaspeFilter("importo"),
        getNullColumn("codicebanca"), getNullColumn("ndgprincipale"), getNullColumn("datainiziodef"))

    // 147

    // JOIN fpasperd_other_gen BY (cd_istituto, ndg, datacont) LEFT, fpasperd_between_out BY (cd_istituto, ndg, datacont);
    // FILTER BY fpasperd_between_out::cd_istituto IS NULL
    val fpasperdOtherOut = fpasperdOtherGen.join(fpasperdBetweenOut, Seq("cd_istituto", "ndg", "datacont"), "left")
      .filter(fpasperdBetweenOut("cd_istituto").isNull)
      .select(fpasperdOtherGen("cd_istituto"), fpasperdOtherGen("ndg"), fpasperdOtherGen("datacont"), fpasperdOtherGen("causale"),
        fpasperdOtherGen("importo"), fpasperdOtherGen("codicebanca"), fpasperdOtherGen("ndgprincipale"), fpasperdOtherGen("datainiziodef"))

    // 170

    // FILTER BY tlbcidef::codicebanca IS NULL
    val fpasperdNullOut = tlbcidefTlbpaspeFilterJoin
      .filter(tlbcidef("codicebanca").isNull)
      .select(tlbpaspeFilter("cd_istituto"), tlbpaspeFilter("ndg"), tlbpaspeFilter("datacont"), tlbpaspeFilter("causale"),
        tlbpaspeFilter("importo"), getNullColumn("codicebanca"), getNullColumn("ndgprincipale"),
        getNullColumn("datainiziodef"))

    // 190
  }

  private def substringAndToInt(column: Column, startIndex: Int, length: Int): Column =
    substring(column, startIndex, length).cast(DataTypes.IntegerType)

  private def getNullColumn(columnName: String): Column = lit(null).cast(DataTypes.StringType).as(columnName)
}
