name := "fm-lazyseq"

description := "LazySeq"

scalaVersion := "3.2.2"

crossScalaVersions := Seq("3.2.2", "2.13.10", "2.12.17", "2.11.12")

val fatalWarnings = Seq(
  // Enable -Xlint, but disable the default 'unused' so we can manually specify below
  "-Xlint:-unused",
  // Remove "params" since we often have method signatures that intentionally have the parameters, but may not be used in every implementation, also omit "patvars" since it isn't part of the default xlint:unused and isn't super helpful
  "-Ywarn-unused:imports,privates,locals",
  // Warnings become Errors
  "-Xfatal-warnings"
)

scalacOptions := Seq(
  "-unchecked",
  "-deprecation",
  "-language:implicitConversions",
  "-feature",
  "-Xlint",
) ++ (if (scalaVersion.value.startsWith("2.11")) Seq(
  // Scala 2.11 specific compiler flags
  "-Ywarn-unused-import"
) else Nil) ++ (if (scalaVersion.value.startsWith("2.12") || scalaVersion.value.startsWith("2.13")) Seq(
  // Scala 2.12/2.13 specific compiler flags
  "-opt:l:inline",
  "-opt-inline-from:<sources>"
) ++ fatalWarnings else Nil) ++ (if (scalaVersion.value.startsWith("3")) Seq(
  //"-Yno-decode-stacktraces"
) else Nil)

// Due to Scala Standard Library changes in 2.13 some code is specific to
// Scala 2.12 and below (e.g. 2.11, 2.12) and some code is specific to 2.13
// and higher (e.g. 2.13, 3.0).
Compile / unmanagedSourceDirectories += {   
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, n)) if n < 13 => sourceDirectory.value / "main" / "scala-2.12-"
    case _ => sourceDirectory.value / "main" / "scala-2.13+"
  }
}

// -Ywarn-unused-import/-Xfatal-warnings casues issues in the REPL and also during doc generation
Compile / console / scalacOptions --= fatalWarnings
Test / console / scalacOptions --= fatalWarnings
Compile / doc / scalacOptions --= fatalWarnings

libraryDependencies ++= Seq(
  "com.frugalmechanic" %% "fm-common" % "1.0.0",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
)

publishTo := sonatypePublishToBundle.value
