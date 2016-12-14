lazy val root = (project in file(".")).
settings(
  name := "bloom-filter-perf",
  version := "1.0",
  scalaVersion := "2.11.8"
)

enablePlugins(JavaAppPackaging)

libraryDependencies ++= Seq(
  "org.roaringbitmap" % "RoaringBitmap" % "0.6.9",
  "me.lemire.integercompression" % "JavaFastPFOR" % "0.1.9",
  "com.esotericsoftware" % "kryo" % "4.0.0"
)
