package it.carloni.luca.lgd.common.utils

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
  }

}
