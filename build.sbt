name := "ScalaMongoDiff"
version := "1.0.1"
organization := "bireme"
scalaVersion := "2.13.10"

publishMavenStyle := true

val mongoScalaDriver = "4.9.0"
val commonsCsv = "1.10.0"
val json4sNative = "4.1.0-M1"
val stringUtils = "3.12.0"

libraryDependencies ++= Seq(
  "org.mongodb.scala" %% "mongo-scala-driver" % mongoScalaDriver,
  "org.apache.commons" % "commons-csv" % commonsCsv,
  "org.json4s" %% "json4s-native" % json4sNative,
  "org.apache.commons" % "commons-lang3" % stringUtils,
)