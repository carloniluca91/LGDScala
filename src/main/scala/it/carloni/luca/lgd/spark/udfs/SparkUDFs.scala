package it.carloni.luca.lgd.spark.udfs

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import scala.util.{Failure, Success, Try}

object SparkUDFs {

  val addDurationUDF: UserDefinedFunction = udf((date: String, datePattern: String, numberOfMonths: Int) => {

    Option(date) match {

      case None => null
      case Some(_) =>

        val formatter = DateTimeFormatter.ofPattern(datePattern)
        val dateWithAddedDuration: Try[String] = Try(LocalDate.parse(date, formatter)
          .plusMonths(numberOfMonths)
          .format(formatter))
        dateWithAddedDuration match {

          case Failure(_) => null
          case Success(value) => value
        }}})

  val changeDateFormatFromY2toY4UDF: UserDefinedFunction = udf((date: String, oldPattern: String, newPattern: String) => {

    Option(date) match {

      case None => null
      case Some(value) =>
        Try(LocalDate.parse(value, DateTimeFormatter.ofPattern(oldPattern))) match {

          case Failure(_) => null
          case Success(localDate: LocalDate) =>

            val formatter = DateTimeFormatter.ofPattern(newPattern)
            if (localDate.isAfter(LocalDate.now())) localDate.minusYears(100).format(formatter)
            else localDate.format(formatter)
        }
    }
  })

  val changeDateFormatUDF: UserDefinedFunction = udf((date: String, oldPattern: String, newPattern: String) => {

    Option(date) match {

      case None => null
      case Some(_) =>

        val dateInNewFormat: Try[String] = Try(LocalDate.parse(date, DateTimeFormatter.ofPattern(oldPattern))
          .format(DateTimeFormatter.ofPattern(newPattern)))
        dateInNewFormat match {

          case Failure(_) => null
          case Success(value) => value
        }}})

  val daysBetweenUDF: UserDefinedFunction = udf((firstDate: String, secondDate: String, commonPattern: String) => {

    val optionFirstDate: Option[String] = Option(firstDate)
    val optionSecondDate: Option[String] = Option(secondDate)
    val nullAsLong: Long = null.asInstanceOf[Long]

    (optionFirstDate, optionSecondDate) match {

      case (Some(_), Some(_)) =>

        val commonDateFormatter = DateTimeFormatter.ofPattern(commonPattern)
        val firstDateLocalDate: Try[LocalDate] = Try(LocalDate.parse(firstDate, commonDateFormatter))
        val secondDateLocalDate: Try[LocalDate] = Try(LocalDate.parse(secondDate, commonDateFormatter))

        firstDateLocalDate match {

          case Failure(_) => nullAsLong
          case Success(firstDateValue) => secondDateLocalDate match {

            case Failure(_) => nullAsLong
            case Success(secondDateValue) => Math.abs(firstDateValue
              .minusDays(secondDateValue.toEpochDay)
              .toEpochDay)
          }}

      case (_, _) => nullAsLong
    }})

  val leastDateUDF: UserDefinedFunction = udf((firstDate: String, secondDate: String, commonDateFormat: String) => {

    val optionFirstDate: Option[String] = Option(firstDate)
    val optionSecondDate: Option[String] = Option(secondDate)
    (optionFirstDate, optionSecondDate) match {

      case (Some(_), Some(_)) =>

        val commonDateFormatter = DateTimeFormatter.ofPattern(commonDateFormat)
        val firstDateLocalDate: Try[LocalDate] = Try(LocalDate.parse(firstDate, commonDateFormatter))
        val secondDateLocalDate: Try[LocalDate] = Try(LocalDate.parse(secondDate, commonDateFormatter))
        firstDateLocalDate match {

          case Failure(_) => null
          case Success(firstDateValue) => secondDateLocalDate match {

            case Failure(_) => null
            case Success(secondDateValue) => if (firstDateValue.isBefore(secondDateValue)) firstDate else secondDate
          }}

      case (_, _) => null
    }})

  val subtractDurationUDF: UserDefinedFunction = udf((date: String, datePattern: String, numberOfMonths: Int) => {

    Option(date) match {

      case None => null
      case Some(_) =>

        val formatter = DateTimeFormatter.ofPattern(datePattern)
        val dateWithSubtractedDuration: Try[String] = Try(LocalDate.parse(date, formatter)
          .minusMonths(numberOfMonths)
          .format(formatter))

        dateWithSubtractedDuration match {

          case Failure(_) => null
          case Success(value) => value
        }}})
}
