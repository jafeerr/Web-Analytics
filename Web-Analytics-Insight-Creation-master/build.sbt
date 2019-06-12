name := "Web-Analytics-Insight-Creation"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.2",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "org.apache.spark" %% "spark-sql" % "2.2.1",
  "org.apache.spark" %% "spark-core" % "2.2.1",
  "com.typesafe" % "config" % "1.2.1",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.0")
assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case _ => MergeStrategy.first
}