FMPublic

name := "fm-lazyseq"

version := "0.2.0-SNAPSHOT"

description := "LazySeq"

scalaVersion := "2.10.4"

crossScalaVersions := Seq("2.10.4", "2.11.0")

scalacOptions := Seq("-unchecked", "-deprecation", "-language:implicitConversions", "-feature", "-optimise", "-Yinline-warnings")

libraryDependencies += "com.frugalmechanic" %% "fm-common" % "0.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.3" % "test"
