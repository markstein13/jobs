
scalaVersion := "2.12.10"

name := "jobs"
organization := "raronson"
version := "1.0"

val sparkVersion = "2.4.5"

libraryDependencies ++= List(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

assemblyJarName in assembly := s"spark-app-${version.value}.jar"

mainClass in assembly := Some("com.swipejobs.App")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case x => MergeStrategy.first
}
