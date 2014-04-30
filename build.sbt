name := "finagle-aws"

organization := "com.heroku"

version := "6.10.0.2-SNAPSHOT"

scalaVersion := "2.10.2"

parallelExecution in Test := false

resolvers ++= Seq("com.thefactory" at "http://maven.thefactory.com/nexus/content/repositories/releases/",
                  "com.thefactory snapshots" at "http://maven.thefactory.com/nexus/content/repositories/snapshots/")

publishMavenStyle := true

credentials += Credentials(Path.userHome / ".thefactory" / "credentials")

publishTo <<= version { (v: String) =>
  val nexus = "http://maven.thefactory.com/nexus/content/repositories/"
  if (v.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "snapshots")
  else
    Some("releases"  at nexus + "releases")
}

libraryDependencies ++= Seq(
   "com.twitter" %% "finagle-core" % "6.10.0",
   "com.twitter" %% "finagle-http" % "6.10.0",
   "joda-time" % "joda-time" % "1.6.2",
   "org.scalatest" %% "scalatest" % "1.9.1" % "test"
)
