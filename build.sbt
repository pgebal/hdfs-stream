lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.pawelgebal",
      scalaVersion := "2.12.3",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "hdfs-text-stream",
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-client" % "2.7.3",
      "com.typesafe.akka" %% "akka-stream" % "2.5.4"
    )
  )
