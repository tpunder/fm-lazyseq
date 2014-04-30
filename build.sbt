name := "fm-lazyseq"

organization := "com.frugalmechanic"

version := "0.2.0-SNAPSHOT"

description := "LazySeq"

licenses := Seq("Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

homepage := Some(url("https://github.com/frugalmechanic/fm-lazyseq"))

scalaVersion := "2.10.4"

// Note: Use "++ 2.11.0" to select a specific version when building
crossScalaVersions := Seq("2.10.4", "2.11.0")

scalacOptions := Seq("-unchecked", "-deprecation", "-language:implicitConversions", "-feature", "-optimise", "-Yinline-warnings")

resolvers ++= Seq(
  "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases/"
)

libraryDependencies += "com.frugalmechanic" %% "fm-common" % "0.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.3" % "test"

publishMavenStyle := true

publishTo <<= version { (v: String) =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT")) 
    Some("snapshots" at nexus + "content/repositories/snapshots") 
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

pomExtra := (
  <developers>
    <developer>
      <id>tim</id>
      <name>Tim Underwood</name>
      <email>tim@frugalmechanic.com</email>
      <organization>Frugal Mechanic</organization>
      <organizationUrl>http://frugalmechanic.com</organizationUrl>
    </developer>
  </developers>
  <scm>
      <connection>scm:git:git@github.com:frugalmechanic/fm-lazyseq.git</connection>
      <developerConnection>scm:git:git@github.com:frugalmechanic/fm-lazyseq.git</developerConnection>
      <url>git@github.com:frugalmechanic/fm-lazyseq.git</url>
  </scm>)

