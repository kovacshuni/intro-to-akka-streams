organization := """com.hunorkovacs"""

name := """riptube"""

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.11.6"

libraryDependencies ++= List(
  "com.typesafe.akka" %% "akka-http-experimental" % "1.0-RC4",
  "org.slf4j" % "slf4j-api" % "1.7.12",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "org.specs2" %% "specs2-core" % "3.6.1" % "test",
  "com.hunorkovacs" %% "work-pulling-actors" % "1.0.0-SNAPSHOT"
)
