package it.carloni.luca.lgd.common.utils

import it.carloni.luca.lgd.common.udfs.UDFsNames
import org.apache.spark.sql.functions.{callUDF, lit, regexp_replace}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Column, Dataset, Row}

object SparkUtils {

  def addDuration(dateColumn: Column, dateColumnFormat: String, numberOfMonths: Int): Column =
    callUDF(UDFsNames.AddDurationUDFName, dateColumn, lit(dateColumnFormat), lit(numberOfMonths))

  def changeDateFormat(dateColumn: Column, oldPattern: String, newPattern: String): Column =
    callUDF(UDFsNames.ChangeDateFormatUDFName, dateColumn, lit(oldPattern), lit(newPattern))

  def colSeq(dataset: Dataset[Row], columns: String*): Seq[Column] = columns map {dataset(_)}

  def isDateGeqOtherDate(dateColumn: Column, dateColumnFormat: String,
                         otherDateColumn: Column, otherDateColumnFormat: String): Column =
    callUDF(UDFsNames.IsDateGeqOtherDateUDFName, dateColumn, lit(dateColumnFormat), otherDateColumn, lit(otherDateColumnFormat))

  def isDateLeqOtherDate(dateColumn: Column, dateColumnFormat: String,
                         otherDateColumn: Column, otherDateColumnFormat: String): Column =
    callUDF(UDFsNames.IsDateLeqOtherDateUDFName, dateColumn, lit(dateColumnFormat), otherDateColumn, lit(otherDateColumnFormat))

  def leastDate(firstDateColumn: Column, secondDateColumn: String, commonDatePattern: String): Column =
    callUDF(UDFsNames.LeastDateUDFName, firstDateColumn, lit(secondDateColumn), lit(commonDatePattern))

  def replaceAndToDouble(column: Column, oldString: String, newString: String): Column =
    regexp_replace(column, oldString, newString).cast(DataTypes.DoubleType)

  def subtractDuration(dateColumn: Column, dateColumnFormat: String, numberOfMonths: Int): Column =
    callUDF(UDFsNames.SubtractDurationUDFName, dateColumn, lit(dateColumnFormat), lit(numberOfMonths))

  def toIntType(column: Column): Column = column.cast(DataTypes.IntegerType)

  def toStringType(column: Column): Column = column.cast(DataTypes.StringType)

}
