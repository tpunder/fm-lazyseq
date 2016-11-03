FMPublic

name := "fm-lazyseq"

version := "0.6.0-SNAPSHOT"

description := "LazySeq"

scalaVersion := "2.12.1"

crossScalaVersions := Seq("2.11.8", "2.12.1")

scalacOptions := Seq(
  "-unchecked",
  "-deprecation",
  "-language:implicitConversions",
  "-feature",
  "-Xlint",
  "-Ywarn-unused-import"
) ++ (if (scalaVersion.value.startsWith("2.12")) Seq(
  // Scala 2.12 specific compiler flags
  "-opt:l:classpath"
) else Nil)

libraryDependencies += "com.frugalmechanic" %% "fm-common" % "0.8.0-SNAPSHOT"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"
