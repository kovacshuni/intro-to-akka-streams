organization := """com.hunorkovacs"""

name := """intro-to-akka-streams"""

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.11.7"

libraryDependencies ++= List(
  "com.typesafe.akka" %% "akka-http-experimental" % "1.0",
  "org.slf4j" % "slf4j-api" % "1.7.12",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "com.typesafe.phoenix" %% "cinnamon-takipi" % "0.2.0"
)

javaOptions += "-agentlib:TakipiAgent"
