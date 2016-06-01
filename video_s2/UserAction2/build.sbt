name := "UserAction"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "com.github.scopt" %% "scopt" % "3.2.0"
)
    