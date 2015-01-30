FMPublic

name := "fm-lazyseq"

version := "0.2.0"

description := "LazySeq"

scalaVersion := "2.11.5"

crossScalaVersions := Seq("2.10.4", "2.11.5")

scalacOptions := Seq("-unchecked", "-deprecation", "-language:implicitConversions", "-feature", "-Xlint", "-optimise", "-Yinline-warnings")

libraryDependencies += "com.frugalmechanic" %% "fm-common" % "0.2.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.3" % "test"
