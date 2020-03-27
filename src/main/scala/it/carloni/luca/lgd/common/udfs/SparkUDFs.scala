package it.carloni.luca.lgd.common.udfs

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object SparkUDFs {

  val addDurationUDF: UserDefinedFunction = udf((date: String, datePattern: String, numberOfMonths: Int) => {

    if (date ne null) {

      val formatter = DateTimeFormatter.ofPattern(datePattern)
      LocalDate.parse(date, formatter).plusMonths(numberOfMonths).format(formatter)

    } else null })

  val changeDateFormat: UserDefinedFunction = udf((date: String, oldPattern: String, newPattern: String) => {

    if (date ne null) {

      LocalDate.parse(date, DateTimeFormatter.ofPattern(oldPattern))
        .format(DateTimeFormatter.ofPattern(newPattern))
    }
     else null })

  val subtractDurationUDF: UserDefinedFunction = udf((date: String, datePattern: String, numberOfMonths: Int) => {

    if (date ne null) {

      val formatter = DateTimeFormatter.ofPattern(datePattern)
      LocalDate.parse(date, formatter).minusMonths(numberOfMonths).format(formatter)

    } else null })

  val isDateGeqOtherDateUDF: UserDefinedFunction = udf((date: String, dateFormat: String, otherDate: String, otherDateFormat: String) => {

    if ((date ne null) && (otherDate ne null)) {

      val dateLocalDate = LocalDate.parse(date, DateTimeFormatter.ofPattern(dateFormat))
      val otherDateLocalDate = LocalDate.parse(otherDate, DateTimeFormatter.ofPattern(otherDateFormat))
      dateLocalDate.compareTo(otherDateLocalDate) >= 0

    } else false })

  val isDateLeqOtherDateUDF: UserDefinedFunction = udf((date: String, dateFormat: String, otherDate: String, otherDateFormat: String) => {

    if ((date ne null) && (otherDate ne null)) {

      val dateLocalDate = LocalDate.parse(date, DateTimeFormatter.ofPattern(dateFormat))
      val otherDateLocalDate = LocalDate.parse(otherDate, DateTimeFormatter.ofPattern(otherDateFormat))
      dateLocalDate.compareTo(otherDateLocalDate) <= 0

    } else false })

  val leastDateUDF: UserDefinedFunction = udf((firstDate: String, secondDate: String, commonDateFormat: String) => {

    if ((firstDate ne null) && (secondDate ne null)) {

      val commonDateFormatter = DateTimeFormatter.ofPattern(commonDateFormat)
      val firstDateLocalDate = LocalDate.parse(firstDate, commonDateFormatter)
      val secondDateLocalDate = LocalDate.parse(secondDate, commonDateFormatter)
      if (firstDateLocalDate.isBefore(secondDateLocalDate)) firstDate else secondDate

    } else null })

}
