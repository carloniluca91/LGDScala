package it.carloni.luca.lgd.spark.utils

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object ScalaUtils {

  def changeDateFormat(localDate: String, localDateOriginalPattern: String, localDateNewPattern: String): String =
    LocalDate.parse(localDate, DateTimeFormatter.ofPattern(localDateOriginalPattern))
      .format(DateTimeFormatter.ofPattern(localDateNewPattern))

}
