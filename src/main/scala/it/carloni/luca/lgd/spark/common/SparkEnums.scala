package it.carloni.luca.lgd.spark.common

object SparkEnums {

  object CSV extends Enumeration {

    val InputDelimiter: CSV.Value = Value(";")
    val OutputDelimiter: CSV.Value = Value(";")
    val SparkCsvFormat: CSV.Value = Value("com.databricks.spark.csv")
  }

  object DateFormats extends Enumeration {

    val DataAFormat: DateFormats.Value = Value("yyyyMMdd")
    val DataDaFormat: DateFormats.Value = Value("yyyyMMdd")
    val DataOsservazioneFormat: DateFormats.Value = Value("yyyyMMdd")
    val PeriodoFormat: DateFormats.Value = Value("yyyy-MM")
    val Y4M2D2Format: DateFormats.Value = Value("yyyyMMdd")
  }

  object UDFsNames extends Enumeration {

    val AddDurationUDFName: UDFsNames.Value = Value("addDuration")
    val ChangeDateFormatUDFName: UDFsNames.Value = Value("changeDateFormat")
    val DaysBetweenUDFName: UDFsNames.Value = Value("daysBetween")
    val LeastDateUDFName: UDFsNames.Value = Value("leastDate")
    val SubtractDurationUDFName: UDFsNames.Value = Value("subtractDuration")
  }

  object StepNames extends Enumeration {

    val CiclilavStep1: Value = Value("CICLILAV_STEP_1")
    val CicliPreview: Value = Value("CICLI_PREVIEW")
    val FanagMonthly: Value = Value("FANAG_MONTHLY")

  }
}
