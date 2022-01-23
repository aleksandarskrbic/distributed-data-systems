name := "write-ahead-log"
version := "0.1"

lazy val `write-ahead-log` = project
  .in(file("."))
  .settings(
    scalaVersion := "2.13.7",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect"  % "3.3.4",
      "io.circe"      %% "circe-parser" % "0.14.1",
      "co.fs2"        %% "fs2-core"     % "3.2.4",
      "co.fs2"        %% "fs2-io"       % "3.2.4"
    )
  )

addCommandAlias("fmt", "all scalafmtSbt scalafmtAll")
