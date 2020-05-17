package it.carloni.luca.lgd.spark.common

object SparkEnums {

  object CSV extends Enumeration {

    val InputDelimiter: Value = Value(";")
    val OutputDelimiter: Value = Value(";")
    val SparkCsvFormat: Value = Value("com.databricks.spark.csv")
  }

  object DateFormats extends Enumeration {

    val DataAFormat: Value = Value("yyyyMMdd")
    val DataDaFormat: Value = Value("yyyyMMdd")
    val DataOsservazioneFormat: Value = Value("yyyyMMdd")
    val PeriodoFormat: Value = Value("yyyy-MM")
    val Y4M2D2Format: Value = Value("yyyyMMdd")
  }

  object UDFsNames extends Enumeration {

    val AddDurationUDFName: Value = Value("addDuration")
    val ChangeDateFormatUDFName: Value = Value("changeDateFormat")
    val ChangeDateFormatFromY2toY4UDFName: Value = Value("changeDateFormatFromY2toY4")
    val DaysBetweenUDFName: Value = Value("daysBetween")
    val LeastDateUDFName: Value = Value("leastDate")
    val SubtractDurationUDFName: Value = Value("subtractDuration")
  }

  object StepNames extends Enumeration {

    val CiclilavStep1: Value = Value("CICLILAV_STEP_1")
    val CicliPreview: Value = Value("CICLI_PREVIEW")
    val FanagMonthly: Value = Value("FANAG_MONTHLY")
    val Fpasperd: Value = Value("FPASPERD")
    val FrappNdgMonthly: Value = Value("FRAPP_NDG_MONTHLY")
    val FrappPuma: Value = Value("FRAPP_PUMA")
    val Movimenti: Value = Value("MOVIMENTI")
    val Posaggr: Value = Value("POSAGGR")
    val QuadFcoll: Value = Value("QUAD_FCOLL")
    val QuadFcollCicli: Value = Value("QUAD_FCOLL_CICLI")
    val QuadFposi: Value = Value("QUAD_FPOSI")
    val QuadFrapp: Value = Value("QUAD_FRAPP")
    val RaccInc: Value = Value("RACC_INC")
    val SofferenzePreview: Value = Value("SOFFERENZE_PREVIEW")
  }
}
