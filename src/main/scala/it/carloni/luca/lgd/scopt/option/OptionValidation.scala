package it.carloni.luca.lgd.scopt.option

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import it.carloni.luca.lgd.spark.common.SparkEnums.DateFormats

import scala.util.Try


object OptionValidation {

  def validateDataDa(inputDate:String): Boolean = validateInputDate(inputDate, DateFormats.DataDaFormat.toString)

  def validateDataA(inputDate:String): Boolean = validateInputDate(inputDate, DateFormats.DataAFormat.toString)

  def validateDataOsservazione(inputDate:String): Boolean = validateInputDate(inputDate, DateFormats.DataOsservazioneFormat.toString)

  private def validateInputDate(inputDate: String, inputDatePattern: String): Boolean = {

    val parseLocalDateTry: Try[LocalDate] = Try(LocalDate.parse(inputDate, DateTimeFormatter.ofPattern(inputDatePattern)))
    parseLocalDateTry.isSuccess
  }

}
