package it.carloni.luca.lgd.common.utils

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.types.{DataType, DataTypes, StructField}

object ScalaUtils {

  def getTypedStructField(tuple2: (String, String)): StructField = StructField(tuple2._1, resolveDataType(tuple2._2))

  def changeLocalDateFormat(localDate: String, localDateOriginalPattern: String, localDateNewPattern: String): String =
    LocalDate.parse(localDate, DateTimeFormatter.ofPattern(localDateOriginalPattern))
      .format(DateTimeFormatter.ofPattern(localDateNewPattern))

  private def resolveDataType(columnType: String): DataType = columnType match {

    case "chararray" => DataTypes.StringType
    case "int" => DataTypes.IntegerType
    case "double" => DataTypes.DoubleType
    case _ => throw new Exception(f"Mismatched data type: $columnType")
  }
}
