package io.experiment

import org.apache.spark.sql.types.{DataType, StringType, IntegerType, LongType, DoubleType, BooleanType, DateType, TimestampType, DecimalType, StructField, StructType}
import org.apache.spark.sql.Row

case class ColumnDefinition(
  column: String,
  `type`: String,
  max_length: Option[Int],
  `null?`: Boolean
)

object ColumnDefinition {
  private val typeMapping: Map[String, DataType] = Map(
    "int2" -> IntegerType,
    "int4" -> IntegerType,
    "int8" -> LongType,
    "float4" -> DoubleType,
    "float8" -> DoubleType,
    "numeric" -> DecimalType(38, 18),
    "decimal" -> DecimalType(38, 18),
    
    "text" -> StringType,
    "varchar" -> StringType,
    "char" -> StringType,
    "bpchar" -> StringType,
    
    "bool" -> BooleanType,
    "boolean" -> BooleanType,
    
    "date" -> DateType,
    "timestamp" -> TimestampType,
    "timestamptz" -> TimestampType,
    "time" -> StringType,
    
    "json" -> StringType,
    "jsonb" -> StringType,
    "uuid" -> StringType,
    "bytea" -> StringType
  )

  def toSparkType(typeName: String): DataType = {
    typeMapping.getOrElse(typeName.toLowerCase, {
      typeName.toLowerCase match {
        case s if s.startsWith("varchar") => StringType
        case s if s.startsWith("char") => StringType
        case s if s.startsWith("numeric") || s.startsWith("decimal") => DecimalType(38, 18)
        case other => throw new IllegalArgumentException(s"Unsupported type: $other")
      }
    })
  }

  def convertValue(value: Any, dataType: DataType, columnName: String): Any = {
    (value, dataType) match {
      case (null, _) => null
      case (v: Number, LongType) => v.longValue()
      case (v: Number, IntegerType) => v.intValue()
      case (v: Number, DoubleType) => v.doubleValue()
      case (v: String, StringType) => v
      case (v: Boolean, BooleanType) => v
      case (v: String, TimestampType) => java.sql.Timestamp.valueOf(v)
      case (v, t) => throw new IllegalArgumentException(s"Unsupported value type ${v.getClass} for schema type $t in column $columnName")
    }
  }

  def convertRow(row: Seq[Any], schema: StructType): Row = {
    val convertedValues = row.zipWithIndex.map { case (value, idx) =>
      val field = schema.fields(idx)
      convertValue(value, field.dataType, field.name)
    }
    Row.fromSeq(convertedValues)
  }
}

case class DataCommand(
  command: String,
  sql: Option[String] = None,
  schema: Option[Seq[ColumnDefinition]] = None,
  data: Option[Seq[Seq[Any]]] = None,
  table: Option[String] = None
) {
  def toSparkSchema: StructType = {
    schema.map { schemaSeq =>
      StructType(
        schemaSeq.map { colDef =>
          StructField(
            colDef.column,
            ColumnDefinition.toSparkType(colDef.`type`),
            colDef.`null?`
          )
        }
      )
    }.getOrElse(throw new IllegalArgumentException("Schema is required for DataFrame operations"))
  }
}

sealed trait Response
case class ReadyResponse(status: String = "ready", message: String = "Waiting for command") extends Response
case class CommandResponse(columns: Seq[String], num_rows: Long, rows: Any) extends Response
case class ErrorResponse(error: String) extends Response 
