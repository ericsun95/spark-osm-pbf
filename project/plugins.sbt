addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.0-RC2")
addSbtPlugin("com.github.gseitz" % "sbt-protobuf" % "0.6.5")
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0" excludeAll(
  ExclusionRule(organization = "com.danieltrinh")))
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.6.1")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.9.8"

