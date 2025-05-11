ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

// Set Java version
ThisBuild / javacOptions ++= Seq("-source", "11", "-target", "11")
ThisBuild / initialize := {
  val _ = initialize.value
  val javaVersion = sys.props("java.specification.version")
  if (javaVersion != "11")
    sys.error(s"Java 11 is required for this project. Found $javaVersion instead")
}

lazy val root = (project in file("."))
  .settings(
    name := "ice",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.4.1",
      "org.apache.spark" %% "spark-sql" % "3.4.1",
      "org.apache.spark" %% "spark-hive" % "3.4.1",
      "org.apache.iceberg" % "iceberg-spark-runtime-3.4_2.13" % "1.4.3",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.3",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.3"
    ),
    assembly / mainClass := Some("Main"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case "log4j2.properties" | "log4j2.xml" | "log4j.properties" | "log4j.xml" => MergeStrategy.first
      case x if x.endsWith(".properties") => MergeStrategy.first
      case x if x.endsWith(".xml") => MergeStrategy.first
      case x if x.endsWith(".types") => MergeStrategy.first
      case x if x.endsWith(".class") => MergeStrategy.first
      case _ => MergeStrategy.first
    }
  )
