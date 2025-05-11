package io.experiment

case class SqlCommand(
  command: String,
  sql: String
)

sealed trait Response
case class ReadyResponse(status: String = "ready", message: String = "Waiting for command") extends Response
case class CommandResponse(columns: Seq[String], num_rows: Long, rows: Any) extends Response
case class ErrorResponse(error: String) extends Response 
