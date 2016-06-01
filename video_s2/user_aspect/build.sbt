name := "standalone"

version := "1.0"

scalaVersion := "2.11.7"
resolvers ++= Seq(
  "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases"
)


libraryDependencies ++= Seq(
  "org.scala-saddle" %% "saddle-core" % "1.3.+",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "com.github.scopt" %% "scopt" % "3.2.0"
)
