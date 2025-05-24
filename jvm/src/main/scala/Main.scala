import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.experiment.{DataCommand, ReadyResponse, CommandResponse, ErrorResponse, ColumnDefinition}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, IntegerType, StringType, DoubleType, BooleanType, TimestampType, StructType, StructField}

import scala.io.StdIn
import scala.util.{Try, Success, Failure}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.ArrayList

object Main {
  def main(args: Array[String]): Unit = {
    println("Started with args: " + args.mkString(", "))
    val warehousePath = args(0)
    val catalogName = args(1)
    val executorsCount = args(2).toInt
    val spark = SparkSession.builder()
      .appName("Iceberg Writer")
      .master(s"local[$executorsCount]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config(s"spark.sql.catalog.$catalogName", "org.apache.iceberg.spark.SparkCatalog")
      .config(s"spark.sql.catalog.$catalogName.type", "hadoop")
      .config(s"spark.sql.catalog.$catalogName.warehouse", warehousePath)
      .config("spark.sql.warehouse.dir", warehousePath)
      .config("spark.hadoop.fs.defaultFS", "file:///")
      .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.fs.LocalFileSystem")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
      .config("spark.sql.iceberg.write.target-file-size-bytes", "134217728")
      .getOrCreate()

    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)

    try {
      var continue = true
      val reader = new java.io.BufferedReader(new java.io.InputStreamReader(System.in))
      
      println(objectMapper.writeValueAsString(ReadyResponse()))
      
      while (continue) {
        val input = reader.readLine()
        if (input == null || input.trim.isEmpty) {
          println(objectMapper.writeValueAsString(ErrorResponse("Empty or null input detected. Exiting...")))
          continue = false
        } else {
          try {
            val dataCommand = objectMapper.readValue(input, classOf[DataCommand])
            dataCommand.command match {
              case "sql" =>
                val sql = dataCommand.sql.getOrElse(throw new IllegalArgumentException("SQL query is required for SQL command"))
                val result = spark.sql(sql)
                val schema = result.schema
                val columns = schema.fields.map(_.name)
                val rows = result.collect().map(row => row.toSeq.toArray)
                val num_rows = rows.length
                
                val response = CommandResponse(columns, num_rows, rows)
                println(objectMapper.writeValueAsString(response))
                
              case "dataframe" =>
                val schema = dataCommand.toSparkSchema
                val data = dataCommand.data.getOrElse(throw new IllegalArgumentException("Data is required for DataFrame command"))
                val table = dataCommand.table.getOrElse(throw new IllegalArgumentException("Table name is required for DataFrame command"))
                
                val javaRows = new ArrayList[Row]()
                data.foreach { row =>
                  val convertedValues = row.zipWithIndex.map { case (value, idx) =>
                    val field = schema.fields(idx)
                    ColumnDefinition.convertValue(value, field.dataType, field.name)
                  }
                  javaRows.add(Row.fromSeq(convertedValues))
                }
                
                spark.createDataFrame(javaRows, schema)
                  .writeTo(table)
                  .append()
                
                val response = CommandResponse(
                  columns = schema.fieldNames.toSeq,
                  num_rows = data.length,
                  rows = Array.empty
                )
                println(objectMapper.writeValueAsString(response))
                
              case other =>
                val errorResponse = ErrorResponse(s"Unknown command: $other")
                println(objectMapper.writeValueAsString(errorResponse))
            }
          } catch {
            case e: Exception =>
              println(objectMapper.writeValueAsString(ErrorResponse(e.getMessage)))
          }
        }
      }
    } catch {
      case e: Exception =>
        println(objectMapper.writeValueAsString(ErrorResponse("An error occurred: " + e.getMessage)))
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
