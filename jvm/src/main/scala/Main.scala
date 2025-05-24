import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.experiment.{SqlCommand, ReadyResponse, CommandResponse, ErrorResponse}
import org.apache.spark.sql.SparkSession

import scala.io.StdIn

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
            val sqlCommand = objectMapper.readValue(input, classOf[SqlCommand])
            if (sqlCommand.command == "sql") {
              val result = spark.sql(sqlCommand.sql)
              val schema = result.schema
              val columns = schema.fields.map(_.name)
              val rows = result.collect().map(row => row.toSeq.toArray)
              val num_rows = rows.length
              
              val response = CommandResponse(columns, num_rows, rows)
              println(objectMapper.writeValueAsString(response))
            } else {
              val errorResponse = ErrorResponse(s"Unknown command: ${sqlCommand.command}")
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
