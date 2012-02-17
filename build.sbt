name := "finagle-aws"

organization := "com.heroku"

version := "1.11.1"

scalaVersion := "2.9.1"

resolvers ++= Seq("twitter.com" at "http://maven.twttr.com")

libraryDependencies ++= Seq(
	"com.twitter" % "finagle-core_2.9.1" % "1.11.1" withSources(),
	"com.twitter" % "finagle-http_2.9.1" % "1.11.1" withSources(),
    "joda-time" % "joda-time" % "1.6.2" withSources(),
     "org.specs2" %% "specs2" % "1.8" % "test"
)



