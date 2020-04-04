package it.carloni.luca.lgd.commons

object LGDCommons {

  object CSV {

    final val InputDelimiter = ";"
    final val OutputDelimiter = ";"
    final val SparkCsvFormat = "com.databricks.spark.csv"
  }

  object DatePatterns {

    final val DataAPattern = "yyyyMMdd"
    final val DataDaPattern = "yyyyMMdd"
    final val DataOsservazionePattern = "yyyyMMdd"
    final val PeriodoPattern = "yyyy-MM"

    final val Y4M2D2Pattern = "yyyyMMdd"
  }

  object Scopt {

    final val scoptProgramName = "scopt 3.3.0"
  }

}
