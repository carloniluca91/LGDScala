package it.carloni.luca.lgd.common.udfs

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object UDFsNames {

  final val AddDurationUDFName = "addDuration"
  final val SubtractDurationUDFName = "subtractDuration"
  final val IsDateGeqOtherDateUDFName = "isDateGeqOtherDate"
  final val IsDateLeqOtherDateUDFName = "isDateLeqOtherDate"
  final val LeastDateUDFName = "leastDate"

}

object SparkUDFs {

  val addDurationUDF: UserDefinedFunction = udf((date: String, datePattern: String, numberOfMonths: Int) => {

    if (!(date equals null)) {

      val formatter = DateTimeFormatter.ofPattern(datePattern)
      LocalDate.parse(date, formatter).plusMonths(numberOfMonths).format(formatter)

    } else null })

  val subtractDurationUDF: UserDefinedFunction = udf((date: String, datePattern: String, numberOfMonths: Int) => {

    if (!(date equals null)) {

      val formatter = DateTimeFormatter.ofPattern(datePattern)
      LocalDate.parse(date, formatter).minusMonths(numberOfMonths).format(formatter)

    } else null })

  val isDateGeqOtherDateUDF: UserDefinedFunction = udf((date: String, dateFormat: String, otherDate: String, otherDateFormat: String) => {

    if ((!(date equals null)) && (!(otherDate equals null))) {

      val dateLocalDate = LocalDate.parse(date, DateTimeFormatter.ofPattern(dateFormat))
      val otherDateLocalDate = LocalDate.parse(otherDate, DateTimeFormatter.ofPattern(otherDateFormat))
      dateLocalDate.compareTo(otherDateLocalDate) >= 0

    } else false })

  val isDateLeqOtherDateUDF: UserDefinedFunction = udf((date: String, dateFormat: String, otherDate: String, otherDateFormat: String) => {

    if ((!(date equals null)) && (!(otherDate equals null))) {

      val dateLocalDate = LocalDate.parse(date, DateTimeFormatter.ofPattern(dateFormat))
      val otherDateLocalDate = LocalDate.parse(otherDate, DateTimeFormatter.ofPattern(otherDateFormat))
      dateLocalDate.compareTo(otherDateLocalDate) <= 0

    } else false })

  val leastDateUDF: UserDefinedFunction = udf((firstDate: String, secondDate: String, commonDateFormat: String) => {

    if ((!(firstDate equals null)) && (!(secondDate equals null))) {

      val commonDateFormatter = DateTimeFormatter.ofPattern(commonDateFormat)
      val firstDateLocalDate = LocalDate.parse(firstDate, commonDateFormatter)
      val secondDateLocalDate = LocalDate.parse(secondDate, commonDateFormatter)
      if (firstDateLocalDate.isBefore(secondDateLocalDate)) firstDate else secondDate

    } else null })

}
