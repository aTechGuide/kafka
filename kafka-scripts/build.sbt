name := "kafka-scripts"
version := "0.1"
scalaVersion := "2.12.11"

val kafkaClientVersion = "2.4.1"
val slf4jVersion = "1.7.30"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(

  "org.apache.kafka" % "kafka-clients" % kafkaClientVersion,
  "org.slf4j" % "slf4j-simple" % slf4jVersion
)

