name := "Inequality-ETL"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.8"

mainClass := Some("ETL")

libraryDependencies ++= Seq(
  "org.locationtech.geotrellis" %% "geotrellis-vector" % "1.0.0",
  "org.locationtech.geotrellis" %% "geotrellis-shapefile" % "1.0.0",
  "org.locationtech.geotrellis" %% "geotrellis-spark" % "1.0.0",
  "org.locationtech.geotrellis" %% "geotrellis-proj4" % "1.0.0",
  "com.amazonaws" % "aws-java-sdk" % "1.7.4",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.0.0",
  ("org.apache.hadoop" % "hadoop-aws" % "2.7.2").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections"),
  "com.github.tototoshi" %% "scala-csv" % "1.3.4",
  "com.databricks" %% "spark-csv" % "1.5.0",
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided"
)


resolvers ++= Seq[Resolver](
  "LocationTech GeoTrellis Snapshots" at "https://repo.locationtech.org/content/repositories/geotrellis-snapshots",
  "Geotools" at "http://download.osgeo.org/webdav/geotools/"
)

assemblyMergeStrategy in assembly := {
  case PathList("com", "vividsolutions", xs @ _*) => MergeStrategy.first
  // case PathList("org", "spire-math", xs @ _*) => MergeStrategy.first
   case x: String =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
