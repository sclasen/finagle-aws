name := "finagle-aws"

organization := "com.heroku"

version := "6.10.0"

scalaVersion := "2.10.2"

parallelExecution in Test := false

resolvers ++= Seq("twitter.com" at "http://maven.twttr.com")

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))

libraryDependencies ++= Seq(
   "com.twitter" %% "finagle-core" % "6.10.0" withSources(),
   "com.twitter" %% "finagle-http" % "6.10.0" withSources(),
   "joda-time" % "joda-time" % "1.6.2" withSources(),
   "org.scalatest" %% "scalatest" % "1.9.1" % "test"
)
