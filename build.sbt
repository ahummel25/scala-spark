name := "HDFS-Load"

version := "0.1"

scalaVersion := "2.11.11"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.apache.spark" %% "spark-hive" % "2.4.4"
)

fork in run := true
