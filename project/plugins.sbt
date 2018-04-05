logLevel := Level.Warn

resolvers += Resolver.sonatypeRepo("releases")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.13.0")
addSbtPlugin("com.eed3si9n" %% "sbt-buildinfo" % "0.6.1")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.2.0-M8")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")

