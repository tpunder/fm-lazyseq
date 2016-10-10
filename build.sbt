FMPublic

name := "fm-lazyseq"

version := "0.5.0-SNAPSHOT"

description := "LazySeq"

scalaVersion := "2.11.8"

scalacOptions := Seq("-unchecked", "-deprecation", "-language:implicitConversions", "-feature", "-Xlint", "-optimise", "-Yinline-warnings")

libraryDependencies += "com.frugalmechanic" %% "fm-common" % "0.7.0-SNAPSHOT"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"
