FMPublic

name := "fm-lazyseq"

version := "0.2.0-SNAPSHOT"

description := "LazySeq"

scalaVersion := "2.11.1"

crossScalaVersions := Seq("2.10.4", "2.11.1")

scalacOptions := Seq("-unchecked", "-deprecation", "-language:implicitConversions", "-feature", "-Xlint", "-optimise", "-Yinline-warnings")

libraryDependencies += "com.frugalmechanic" %% "fm-common" % "0.2.0-SNAPSHOT"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.3" % "test"
