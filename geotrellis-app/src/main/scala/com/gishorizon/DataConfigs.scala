package com.gishorizon

object DataConfigs {
  val Layers = Map{
    "bathymetry" -> Map(
      "path" -> "data/out/bathymetry",
      "render" -> "terrain"
    )
    "liss3" -> Map(
      "path" -> "data/out/liss3",
      "render" -> "terrain"
    )
  }

  val RenderStyles = Map(
    "terrain" -> Map(
      "min" -> -7000,
      "max" -> 8000,
      "numSteps" -> 100
    )
  )
  var OUT_PATH = "G:/ProjectData/geoprocess/"
  var TEMP_PATH = "G:/ProjectData/temp_data/"
  var TILE_PATH = "G:/ProjectData/tiles/"
  var DATA_HOST = "http://localhost:8082"
  var SPARK_MASTER = "spark://130.211.234.246:7077"
  var EXE_MEM = "12g"
  var DRI_MEM = "12g"
}
