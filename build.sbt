name := "aecor-foundationdb-journal"

organization := "io.aecor"

version := "0.1-SNAPSHOT"

scalaVersion := "2.12.4"

scalafmtOnCompile := true

lazy val kindProjectorVersion = "0.9.4"
lazy val aecorVersion = "0.17.0-SNAPSHOT"
lazy val scalaCheckVersion = "1.13.4"
lazy val scalaTestVersion = "3.0.1"
lazy val scalaCheckShapelessVersion = "1.1.4"
lazy val catsVersion = "1.1.0"
lazy val catsEffectVersion = "1.0.0-RC"

libraryDependencies ++= Seq(
  "co.fs2" %% "fs2-core" % "0.10.4",
  "org.typelevel" %% "cats-effect" % catsEffectVersion,
  "io.aecor" %% "core" % aecorVersion,
  "org.scalacheck" %% "scalacheck" % scalaCheckVersion % Test,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % scalaCheckShapelessVersion % Test,
  "org.typelevel" %% "cats-testkit" % catsVersion % Test
)

scalacOptions ++= Seq(
  "-J-Xss16m",
  "-deprecation",
  "-encoding",
  "utf-8",
  "-explaintypes",
  "-feature",
  "-language:existentials",
  "-language:experimental.macros",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-unchecked",
  "-Xcheckinit",
  "-Xfatal-warnings",
  "-Xfuture",
  "-Xlint:adapted-args",
  "-Xlint:by-name-right-associative",
  "-Xlint:constant",
  "-Xlint:delayedinit-select",
  "-Xlint:doc-detached",
  "-Xlint:inaccessible",
  "-Xlint:infer-any",
  "-Xlint:missing-interpolator",
  "-Xlint:nullary-override",
  "-Xlint:nullary-unit",
  "-Xlint:option-implicit",
  "-Xlint:package-object-classes",
  "-Xlint:poly-implicit-overload",
  "-Xlint:private-shadow",
  "-Xlint:stars-align",
  "-Xlint:type-parameter-shadow",
  "-Xlint:unsound-match",
  "-Yno-adapted-args",
  "-Ypartial-unification",
  "-Ywarn-dead-code",
  "-Ywarn-extra-implicit",
  "-Ywarn-inaccessible",
  "-Ywarn-infer-any",
  "-Ywarn-nullary-override",
  "-Ywarn-nullary-unit",
  "-Ywarn-numeric-widen",
  "-Ywarn-unused:implicits",
  "-Ywarn-unused:imports",
  "-Ywarn-unused:locals",
  "-Ywarn-unused:params",
  "-Ywarn-unused:patvars",
  "-Ywarn-unused:privates",
  "-Ywarn-value-discard",
  "-Xsource:2.13"
)
addCompilerPlugin("org.spire-math" %% "kind-projector" % kindProjectorVersion)
parallelExecution in Test := false
scalacOptions in (Compile, console) --= Seq("-Ywarn-unused:imports", "-Xfatal-warnings")

publishMavenStyle := true

releaseCrossBuild := true

releaseCommitMessage := s"Set version to ${if (releaseUseGlobalVersion.value) (version in ThisBuild).value
else version.value}"
releaseVersionBump := sbtrelease.Version.Bump.Minor
publishTo := {
  val nexus = "http://nexus.market.local/repository/maven-"
  if (isSnapshot.value)
    Some("snapshots".at(nexus + "snapshots/"))
  else
    Some("releases".at(nexus + "releases/"))
}
credentials += Credentials(Path.userHome / ".m2" / "nexus-market-local.credentials")

(fork in Test) := true
