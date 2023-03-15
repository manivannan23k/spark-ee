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
}
