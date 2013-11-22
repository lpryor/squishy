organization := "squishy"

name := "squishy"

version := "3.0"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "atmos" %% "atmos" % "1.1",
  "com.amazonaws" % "aws-java-sdk" % "1.6.4",
  "junit" % "junit" % "4.11" % "test",
  "org.scalatest" %% "scalatest" % "2.0" % "test"
)
