import AssemblyKeys._

assemblySettings

name := "cassie_hector"

version := "1.0"

scalaVersion := "2.9.2"

resolvers ++= Seq("twitter" at "http://maven.twttr.com")

libraryDependencies ++= Seq("com.twitter" % "cassie-core" % "0.23.0",
							"com.twitter" % "util-core" % "4.0.1",
							"me.prettyprint" % "hector-core" % "1.0-5")
