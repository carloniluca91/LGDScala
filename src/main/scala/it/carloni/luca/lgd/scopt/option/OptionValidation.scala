package it.carloni.luca.lgd.scopt.option

import java.time.LocalDate
import java.time.format.{DateTimeFormatter, DateTimeParseException}

import it.carloni.luca.lgd.common.utils.LGDCommons

object OptionValidation {

  def validateDataDa(inputDate:String): Boolean = validateInputDate(inputDate, LGDCommons.DatePatterns.DataDaPattern)

  def validateDataA(inputDate:String): Boolean = validateInputDate(inputDate, LGDCommons.DatePatterns.DataAPattern)

  def validateDataOsservazione(inputDate:String): Boolean = validateInputDate(inputDate, LGDCommons.DatePatterns.DataOsservazionePattern)

  private def validateInputDate(inputDate: String, inputDatePattern: String): Boolean = {

    try {

      val localDateFromDataDa = LocalDate.parse(inputDate, DateTimeFormatter.ofPattern(inputDatePattern))
      true
    }

    catch {

      case _: DateTimeParseException => false
    }
  }

}
