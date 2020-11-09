name := "spark-osm-pbf"
version := "0.1"
scalaVersion := "2.11.12"
//crossScalaVersions := Seq("2.11.12", "2.12.12")
crossScalaVersions := Seq("2.11.12")
scalacOptions := Seq("-unchecked", "-deprecation")
val sparkVersion = sys.props.get("spark.testVersion").getOrElse("2.4.7")
enablePlugins(ProtobufPlugin)

// To avoid packaging it, it's Provided below
autoScalaLibrary := false
libraryDependencies ++= Seq(
  "commons-io" % "commons-io" % "2.7",
  "org.glassfish.jaxb" % "txw2" % "2.3.3",
  "org.slf4j" % "slf4j-api" % "1.7.25" % Provided,
  "org.scalatest" %% "scalatest" % "3.2.2" % Test,
  "com.novocode" % "junit-interface" % "0.11" % Test,
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.scala-lang" % "scala-library" % scalaVersion.value % Provided
)

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value / "scalapb"
)

// (optional) If you need scalapb/scalapb.proto or anything from
// google/protobuf/*.proto
libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
)