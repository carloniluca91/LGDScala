package it.carloni.luca.lgd.spark.step

import it.carloni.luca.lgd.schema.FpasperdSchema
import it.carloni.luca.lgd.scopt.config.EmptyConfig
import it.carloni.luca.lgd.spark.common.AbstractSparkStep
import it.carloni.luca.lgd.spark.utils.SparkUtils.{addDurationUDF, daysBetweenUDF, toIntType}
import org.apache.spark.sql.functions.{coalesce, col, first, lit, substring, when}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DataTypes
import org.apache.log4j.Logger

class Fpasperd extends AbstractSparkStep[EmptyConfig] {

  private val logger = Logger.getLogger(getClass)

  // STEP PATHS
  private val cicliNdgCsvPath = getPropertyValue("fpasperd.cicli.ndg.path.csv")
  private val tlbpaspeFilterCsvPath = getPropertyValue("fpasperd.tlbpaspe.filter.csv")
  private val tlbpaspeossCsvPath = getPropertyValue("fpasperd.tlbpaspeoss.csv")
  private val paspePaspeossGenDistOutputPath = getPropertyValue("fpasperd.paspe.paspeoss.gen.dist")

  // STEP SCHEMAS
  private val tlbcidefLoadPigSchema = FpasperdSchema.tlbcidefLoadPigSchema
  private val tlbpaspeFilterPigSchema = FpasperdSchema.tlbpaspeFilterPigSchema
  private val tlbpaspeossPigSchema = FpasperdSchema.tlbpaspeossPigSchema

  def run(emptyConfig: EmptyConfig): Unit = {

    logger.info(s"fpasperd.cicli.ndg.path.csv: $cicliNdgCsvPath")
    logger.info(s"fpasperd.tlbpaspe.filter.csv: $tlbpaspeFilterCsvPath")
    logger.info(s"fpasperd.tlbpaspeoss.csv: $tlbpaspeossCsvPath")
    logger.info(s"fpasperd.paspe.paspeoss.gen.dist: $paspePaspeossGenDistOutputPath")

    val Y4M2D2Format = "yyyyMMdd"

    // 19
    // ,(int)ToString(AddDuration( ToDate( (chararray)datafinedef,'yyyyMMdd' ),'P2M' ),'yyyyMMdd' )	AS  datafinedef
    val dataFineDefCol = toIntType(addDurationUDF(col("datafinedef"), Y4M2D2Format, 2)).as("datafinedef")

    val tlbcidef = readCsvFromPathUsingSchema(cicliNdgCsvPath, tlbcidefLoadPigSchema)
      .select(col("codicebanca"), col("ndgprincipale"), col("datainiziodef"),
        dataFineDefCol, col("codicebanca_collegato"), col("ndg_collegato"))

    // 56

    val tlbpaspeFilter = readCsvFromPathUsingSchema(tlbpaspeFilterCsvPath, tlbpaspeFilterPigSchema)

    // 71

    // JOIN tlbpaspe_filter BY (cd_istituto, ndg) LEFT, tlbcidef BY (codicebanca_collegato, ndg_collegato);
    val tlbcidefTlbpaspeFilterJoinConditionCol = (tlbpaspeFilter("cd_istituto") === tlbcidef("codicebanca_collegato")) &&
      (tlbpaspeFilter("ndg") === tlbcidef("ndg_collegato"))

    // DaysBetween( ToDate((chararray)tlbcidef::datafinedef,'yyyyMMdd' ), ToDate((chararray)tlbpaspe_filter::datacont,'yyyyMMdd' ) ) as days_diff
    val daysBetweenCol = daysBetweenUDF(tlbcidef("datafinedef"), tlbpaspeFilter("datacont"), Y4M2D2Format).as("days_diff")

    // ... = JOIN tlbpaspe_filter BY (cd_istituto, ndg) LEFT, tlbcidef BY (codicebanca_collegato, ndg_collegato);
    // FILTER ... BY (int)SUBSTRING((chararray)tlbpaspe_filter::datacont,0,6) >= (int)SUBSTRING((chararray)tlbcidef::datainiziodef,0,6)
    // AND (int)SUBSTRING((chararray)tlbpaspe_filter::datacont,0,6) < (int)SUBSTRING( (chararray)tlbcidef::datafinedef,0,6 )

    val fpasperdBetweenGen = tlbpaspeFilter.join(tlbcidef, tlbcidefTlbpaspeFilterJoinConditionCol, "left")
      .filter((substringAndToInt(tlbpaspeFilter("datacont")) >= substringAndToInt(tlbcidef("datainiziodef"))) &&
        (substringAndToInt(tlbpaspeFilter("datacont")) < substringAndToInt(tlbcidef("datafinedef"))))
      .select(tlbpaspeFilter("cd_istituto"), tlbpaspeFilter("ndg"), tlbpaspeFilter("datacont"), tlbpaspeFilter("causale"),
        tlbpaspeFilter("importo"), tlbcidef("codicebanca"), tlbcidef("ndgprincipale"), tlbcidef("datainiziodef"), tlbcidef("datafinedef"),
        daysBetweenCol)

    // 104

    // GROUP fpasperd_between_gen BY ( cd_istituto, ndg, datacont, causale, codicebanca, ndgprincipale );
    // ORDER fpasperd_between_gen by days_diff ASC;

    val fpasperdBetweenOutWindowSpec = Window
      .partitionBy("cd_istituto", "ndg", "datacont", "causale", "codicebanca", "ndgprincipale")
      .orderBy(col("days_diff").asc)

    val fpasperdBetweenOut = fpasperdBetweenGen
      .select(col("cd_istituto"), col("ndg"), col("datacont"), col("causale"),
        first(col("importo")).over(fpasperdBetweenOutWindowSpec).as("importo"),
        col("codicebanca"), col("ndgprincipale"),
        first(col("datainiziodef")).over(fpasperdBetweenOutWindowSpec).as("datainiziodef"))
      .distinct()

    // 127

    // ... = JOIN tlbpaspe_filter BY (cd_istituto, ndg) LEFT, tlbcidef BY (codicebanca_collegato, ndg_collegato);
    // FILTER ... BY tlbcidef::codicebanca IS NOT NULL
    val fpasperdOtherGen = tlbpaspeFilter.join(tlbcidef, tlbcidefTlbpaspeFilterJoinConditionCol, "left_semi")
      .select(tlbpaspeFilter("cd_istituto"), tlbpaspeFilter("ndg"), tlbpaspeFilter("datacont"), tlbpaspeFilter("causale"), tlbpaspeFilter("importo"),
        getNullColumn("codicebanca"), getNullColumn("ndgprincipale"), getNullColumn("datainiziodef"))

    // 147

    // JOIN fpasperd_other_gen BY (cd_istituto, ndg, datacont) LEFT, fpasperd_between_out BY (cd_istituto, ndg, datacont);
    // FILTER BY fpasperd_between_out::cd_istituto IS NULL

    val fpasperdOtherOut = fpasperdOtherGen.join(fpasperdBetweenOut, Seq("cd_istituto", "ndg", "datacont"), "left_anti")

    // 170

    // ... = JOIN tlbpaspe_filter BY (cd_istituto, ndg) LEFT, tlbcidef BY (codicebanca_collegato, ndg_collegato);
    // FILTER ... BY tlbcidef::codicebanca IS NULL

    val fpasperdNullOut = tlbpaspeFilter.join(tlbcidef, tlbcidefTlbpaspeFilterJoinConditionCol, "left_anti")
      .select(tlbpaspeFilter("cd_istituto"), tlbpaspeFilter("ndg"), tlbpaspeFilter("datacont"), tlbpaspeFilter("causale"),
        tlbpaspeFilter("importo"), getNullColumn("codicebanca"), getNullColumn("ndgprincipale"),
        getNullColumn("datainiziodef"))

    // 190

    // JOIN fpasperd_null_out BY (cd_istituto, ndg) LEFT, tlbcidef BY (codicebanca, ndgprincipale);
    val principTlbcidefTlbpaspeFilterJoinConditionCol = (fpasperdNullOut("cd_istituto") === tlbcidef("codicebanca")) &&
      (fpasperdNullOut("ndg") === tlbcidef("ndgprincipale"))

    // FILTER BY (int)SUBSTRING((chararray)fpasperd_null_out::datacont,0,6) >= (int)SUBSTRING((chararray)tlbcidef::datainiziodef,0,6)
    // AND (int)SUBSTRING((chararray)fpasperd_null_out::datacont,0,6) < (int)SUBSTRING( (chararray)tlbcidef::datafinedef,0,6 )

    val dataContBetweenDataInizioAndFineDefFilterColCol = (substringAndToInt(fpasperdNullOut("datacont")) >=
      substringAndToInt(tlbcidef("datainiziodef"))) &&
      (substringAndToInt(fpasperdNullOut("datacont")) <
        substringAndToInt(tlbcidef("datafinedef")))

    // ,DaysBetween( ToDate((chararray)tlbcidef::datafinedef,'yyyyMMdd' ), ToDate((chararray)fpasperd_null_out::datacont,'yyyyMMdd' ) ) as days_diff
    val daysDiffCol = daysBetweenUDF(tlbcidef("datafinedef"), fpasperdNullOut("datacont"), Y4M2D2Format).as("days_diff")

    val principFpasperdBetweenGen = fpasperdNullOut.join(tlbcidef, principTlbcidefTlbpaspeFilterJoinConditionCol, "left")
      .filter(dataContBetweenDataInizioAndFineDefFilterColCol)
      .select(fpasperdNullOut("cd_istituto"), fpasperdNullOut("ndg"), fpasperdNullOut("datacont"), fpasperdNullOut("causale"),
        fpasperdNullOut("importo"), tlbcidef("codicebanca"), tlbcidef("ndgprincipale"), tlbcidef("datainiziodef"), tlbcidef("datafinedef"),
        daysDiffCol)

    // 222

    val principFpasperdBetweenOutWindowSpec = Window
      .partitionBy("cd_istituto", "ndg", "datacont", "causale", "codicebanca", "ndgprincipale")
      .orderBy(col("days_diff").asc)

    val principFpasperdBetweenOut = principFpasperdBetweenGen
      .select(col("cd_istituto"), col("ndg"), col("datacont"), col("causale"),
        first(col("importo")).over(principFpasperdBetweenOutWindowSpec).as("importo"),
          col("codicebanca"), col("ndgprincipale"),
        first(col("datainiziodef")).over(principFpasperdBetweenOutWindowSpec).as("datainiziodef"))
      .distinct()

    // 245

    // ... = JOIN fpasperd_null_out BY (cd_istituto, ndg) LEFT, tlbcidef BY (codicebanca, ndgprincipale);
    // FILTER ... BY tlbcidef::codicebanca IS NOT NULL

    val principFpasperdOtherGen = fpasperdNullOut.join(tlbcidef, principTlbcidefTlbpaspeFilterJoinConditionCol, "left_semi")
      .select(fpasperdNullOut("cd_istituto"), fpasperdNullOut("ndg"), fpasperdNullOut("datacont"), fpasperdNullOut("causale"),
        fpasperdNullOut("importo"), getNullColumn("codicebanca"), getNullColumn("ndgprincipale"),
        getNullColumn("datainiziodef"))

    // 265

    // JOIN princip_fpasperd_other_gen BY (cd_istituto, ndg, datacont) LEFT, princip_fpasperd_between_out BY (cd_istituto, ndg, datacont);
    // FILTER BY princip_fpasperd_between_out::cd_istituto IS NULL

    val principFpasperdOtherOut = principFpasperdOtherGen.join(principFpasperdBetweenOut, Seq("cd_istituto", "ndg", "datacont"), "left_anti")

    // 288

    // ... = JOIN fpasperd_null_out BY (cd_istituto, ndg) LEFT, tlbcidef BY (codicebanca, ndgprincipale);
    // FILTER ... BY tlbcidef::codicebanca IS NULL

    val principFpasperdNullOut = fpasperdNullOut.join(tlbcidef, principTlbcidefTlbpaspeFilterJoinConditionCol, "left_anti")
      .select(fpasperdNullOut("cd_istituto"), fpasperdNullOut("ndg"), fpasperdNullOut("datacont"), fpasperdNullOut("causale"),
        fpasperdNullOut("importo"), getNullColumn("codicebanca"), getNullColumn("ndgprincipale"),
        getNullColumn("datainiziodef"))

    // 308

    logger.info("Trying to perform union of many DataFrames")

    val dataFrameSeq: Seq[DataFrame] = Seq(fpasperdBetweenOut, fpasperdOtherOut, principFpasperdBetweenOut,
      principFpasperdOtherOut, principFpasperdNullOut)

    for ((df, index) <- dataFrameSeq.zipWithIndex) logger.info(s"DataFrame # $index has ${df.columns.length} columns (${df.columns.mkString(", ")})")

    val fpasperdOutDistinct = dataFrameSeq
      .reduce(_ union _)
      .distinct

    logger.info("Successfully performed DataFrames union")

    // 331

    val tlbpaspeoss = readCsvFromPathUsingSchema(tlbpaspeossCsvPath, tlbpaspeossPigSchema)
      .withColumnRenamed("cd_istituto", "cd_istituto_")
      .withColumnRenamed("ndg", "ndg_")
      .withColumnRenamed("datacont", "datacont_")

    // 344

    // JOIN fpasperd_out_distinct BY (cd_istituto, ndg, datacont) FULL OUTER, tlbpaspeoss BY (cd_istituto, ndg, datacont)
    val paspePaspeossGenDistJoinCondition: Column = Seq("cd_istituto", "ndg", "datacont")
      .map((columnName: String) => fpasperdOutDistinct(columnName) === tlbpaspeoss(columnName.concat("_")))
      .reduce(_ && _)

    // ( fpasperd_out_distinct::cd_istituto is not null? ( tlbpaspeoss::cd_istituto is not null?
    // tlbpaspeoss::cd_istituto : fpasperd_out_distinct::cd_istituto ) : tlbpaspeoss::cd_istituto ) as cd_istituto
    val cdIstitutoCol = when(fpasperdOutDistinct("cd_istituto").isNotNull, coalesce(tlbpaspeoss("cd_istituto_"), fpasperdOutDistinct("cd_istituto")))
      .otherwise(tlbpaspeoss("cd_istituto_")).as("cd_istituto")

    // ,( fpasperd_out_distinct::cd_istituto is not null? ( tlbpaspeoss::cd_istituto is not null?
    // tlbpaspeoss::ndg : fpasperd_out_distinct::ndg ) : tlbpaspeoss::ndg ) as ndg
    val ndgCol = when(fpasperdOutDistinct("cd_istituto").isNotNull, when(tlbpaspeoss("cd_istituto_").isNotNull, tlbpaspeoss("ndg_"))
      .otherwise(fpasperdOutDistinct("ndg"))).otherwise(tlbpaspeoss("ndg_")).as("ndg")

    // ,( fpasperd_out_distinct::cd_istituto is not null? ( tlbpaspeoss::cd_istituto is not null?
    // tlbpaspeoss::datacont : fpasperd_out_distinct::datacont ) : tlbpaspeoss::datacont ) as datacont
    val dataContCol = when(fpasperdOutDistinct("cd_istituto").isNotNull, when(tlbpaspeoss("cd_istituto_").isNotNull, tlbpaspeoss("datacont_"))
      .otherwise(fpasperdOutDistinct("datacont"))).otherwise(tlbpaspeoss("datacont_")).as("datacont")

    // ,( fpasperd_out_distinct::cd_istituto is not null? ( tlbpaspeoss::cd_istituto is not null?
    // tlbpaspeoss::causale : fpasperd_out_distinct::causale ) : tlbpaspeoss::causale ) as causale
    val causaleCol = when(fpasperdOutDistinct("cd_istituto").isNotNull, when(tlbpaspeoss("cd_istituto_").isNotNull, tlbpaspeoss("causale"))
      .otherwise(fpasperdOutDistinct("causale"))).otherwise(tlbpaspeoss("causale")).as("causale")

    // ( fpasperd_out_distinct::cd_istituto is not null? ( tlbpaspeoss::cd_istituto is not null?
    // tlbpaspeoss::importo : fpasperd_out_distinct::importo ) : tlbpaspeoss::importo ) as importo
    val importoCol = when(fpasperdOutDistinct("cd_istituto").isNotNull, when(tlbpaspeoss("cd_istituto_").isNotNull, tlbpaspeoss("importo"))
      .otherwise(fpasperdOutDistinct("importo"))).otherwise(tlbpaspeoss("importo")).as("importo")

    // ( fpasperd_out_distinct::cd_istituto is not null? fpasperd_out_distinct::codicebanca : NULL ) as codicebanca
    val codiceBancaCol = when(fpasperdOutDistinct("cd_istituto").isNotNull, fpasperdOutDistinct("codicebanca")).otherwise(null)
      .cast(DataTypes.StringType).as("codicebanca")

    // ( fpasperd_out_distinct::cd_istituto is not null? fpasperd_out_distinct::ndgprincipale : NULL ) as ndgprincipale
    val ndgPrincipaleCol = when(fpasperdOutDistinct("cd_istituto").isNotNull, fpasperdOutDistinct("ndgprincipale")).otherwise(null)
      .cast(DataTypes.StringType).as("ndgprincipale")

    // ( fpasperd_out_distinct::cd_istituto is not null? fpasperd_out_distinct::datainiziodef : NULL ) as datainiziodef
    val dataInizioDefCol = when(fpasperdOutDistinct("cd_istituto").isNotNull, fpasperdOutDistinct("datainiziodef")).otherwise(null)
      .cast(DataTypes.StringType).as("datainiziodef")

    val paspePaspeossGenDist = fpasperdOutDistinct.join(tlbpaspeoss, paspePaspeossGenDistJoinCondition, "full_outer")
      .select(cdIstitutoCol, ndgCol, dataContCol, causaleCol, importoCol, codiceBancaCol, ndgPrincipaleCol, dataInizioDefCol)
      .distinct

    writeDataFrameAsCsvToPath(paspePaspeossGenDist, paspePaspeossGenDistOutputPath)
  }

  private def substringAndToInt(column: Column): Column =
    substring(column, 0, 6).cast(DataTypes.IntegerType)

  private def getNullColumn(columnName: String): Column = lit(null).cast(DataTypes.StringType).as(columnName)
}
