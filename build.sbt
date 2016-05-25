import sbtassembly.Plugin.AssemblyKeys._

assemblySettings

name := "spark-android-log"

version := "0.0.1"

organization := "com.fortysevendeg"

scalaVersion := "2.10.6"

val sparkVersion = "1.6.1"

val http4sVersion = "0.13.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.kafka" %% "kafka" % "0.9.0.1",
  "org.http4s" %% "http4s-dsl" % http4sVersion,
  "org.http4s" %% "http4s-blaze-server" % http4sVersion,
  "org.http4s" %% "http4s-blaze-client" % http4sVersion,
  "org.http4s" %% "http4s-circe" % http4sVersion,
  "com.typesafe.akka" %% "akka-actor" % "2.3.15",
  "com.101tec" % "zkclient" % "0.8"
)

resolvers ++= Seq(
  Resolver.mavenLocal,
  Resolver.sonatypeRepo("public")
)


mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
