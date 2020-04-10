package it.carloni.luca.lgd.spark.utils

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.types.{DataType, DataTypes, StructField}

object ScalaUtils {

  def changeDateFormat(localDate: String, localDateOriginalPattern: String, localDateNewPattern: String): String =
    LocalDate.parse(localDate, DateTimeFormatter.ofPattern(localDateOriginalPattern))
      .format(DateTimeFormatter.ofPattern(localDateNewPattern))

}
