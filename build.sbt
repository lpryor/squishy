organization := "squishy"

name := "squishy"

version := "3.0"

scalaVersion := "2.10.2"

libraryDependencies ++= Seq(
  "atmos" %% "atmos" % "1.0",
  "com.amazonaws" % "aws-java-sdk" % "1.5.2",
  "junit" % "junit" % "4.11" % "test",
  "org.scalatest" %% "scalatest" % "1.9.1" % "test"
)
