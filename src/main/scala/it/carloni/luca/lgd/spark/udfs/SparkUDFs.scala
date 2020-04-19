package it.carloni.luca.lgd.spark.udfs

import java.time.{DateTimeException, LocalDate}
import java.time.format.{DateTimeFormatter, DateTimeParseException}

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object SparkUDFs {

  val addDurationUDF: UserDefinedFunction = udf((date: String, datePattern: String, numberOfMonths: Int) => {

    try {

      val formatter = DateTimeFormatter.ofPattern(datePattern)
      LocalDate.parse(date, formatter).plusMonths(numberOfMonths).format(formatter)

    } catch {

      case _: NullPointerException => null
      case _: DateTimeParseException => null
      case _: DateTimeException => null
    }})

  val changeDateFormatUDF: UserDefinedFunction = udf((date: String, oldPattern: String, newPattern: String) => {

    try {

      LocalDate.parse(date, DateTimeFormatter.ofPattern(oldPattern))
        .format(DateTimeFormatter.ofPattern(newPattern))

    } catch {

      case _: NullPointerException => null
      case _: DateTimeParseException => null
      case _: DateTimeException => null
    }})

    val daysBetweenUDF: UserDefinedFunction = udf((firstDate: String, secondDate: String, commonPattern: String) => {

    try {

      val dateTimeFormatter = DateTimeFormatter.ofPattern(commonPattern)
      val secondDateInEpochDays = LocalDate.parse(secondDate, dateTimeFormatter).toEpochDay
      Math.abs(LocalDate.parse(firstDate, dateTimeFormatter).minusDays(secondDateInEpochDays).toEpochDay)

      } catch {

      case _: NullPointerException => null
      case _: DateTimeParseException => null
      case _: DateTimeException => null
    }})

  val leastDateUDF: UserDefinedFunction = udf((firstDate: String, secondDate: String, commonDateFormat: String) => {

    try {

      val commonDateFormatter = DateTimeFormatter.ofPattern(commonDateFormat)
      val firstDateLocalDate = LocalDate.parse(firstDate, commonDateFormatter)
      val secondDateLocalDate = LocalDate.parse(secondDate, commonDateFormatter)
      if (firstDateLocalDate.isBefore(secondDateLocalDate)) firstDate else secondDate

    } catch {

      case _: NullPointerException => null
      case _: DateTimeParseException => null
      case _: DateTimeException => null
    }})

  val subtractDurationUDF: UserDefinedFunction = udf((date: String, datePattern: String, numberOfMonths: Int) => {

    try {

      val formatter = DateTimeFormatter.ofPattern(datePattern)
      LocalDate.parse(date, formatter).minusMonths(numberOfMonths).format(formatter)

    } catch {

      case _: NullPointerException => null
      case _: DateTimeParseException => null
      case _: DateTimeException => null
    }})
}
