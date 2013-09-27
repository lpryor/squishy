organization := "squishy"

name := "squishy"

version := "2.1"

scalaVersion := "2.10.2"

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk" % "1.5.2",
  "junit" % "junit" % "4.11" % "test",
  "org.scalatest" %% "scalatest" % "1.9.1" % "test"
)
