name := "Vizi"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "0.9.0-incubating",
  "org.apache.spark" %% "spark-streaming-twitter" % "0.9.0-incubating"
)

// libraryDependencies += "com.github.scopt" %% "scopt" % "3.4.0"

// resolvers += Resolver.sonatypeRepo("public")