package com.gishorizon.operations


import geotrellis.layer.{Bounds, Metadata, SpaceTimeKey, SpatialKey, TemporalKey, TileLayerMetadata}
import geotrellis.raster.{ArrayTile, MultibandTile, Raster, Tile}
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD, RasterSourceRDD, RasterSummary, TileLayerRDD, withTilerMethods, _}
import geotrellis.spark.store.file.FileLayerReader
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.store.{Intersects, LayerId}
import geotrellis.store.file.FileAttributeStore
import com.gishorizon.RddUtils.singleTiffTimeSeriesRdd
import com.gishorizon.{RddUtils, Spark}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.rdd.RDD
import geotrellis.raster.io.geotiff._
import geotrellis.raster.{io => _, _}
import geotrellis.spark.stitch._
import org.joda.time.Interval

import java.awt.Color
import java.awt.image.BufferedImage
import java.io.File
import java.time.{ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter
import javax.imageio.ImageIO
import scala.Double.NaN
import org.joda.time._

object Mosaic {

  def runProcess(inputs: Map[String, RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]], processOperation: ProcessOperation)
  : RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]
  = {
    val startTs = processOperation.params.split("#").head.toLong
    val endTs = processOperation.params.split("#")(1).toLong
    val intervalDays = processOperation.params.split("#").last.toInt
    val intervalDuration = Duration.standardDays(intervalDays)
    var meta: TileLayerMetadata[SpaceTimeKey] = null
    val i = processOperation.inputs(0)
    val m = inputs(i.id).metadata
    meta = m
    val intervalStart = new DateTime(startTs) //new DateTime(1989, 11, 1, 0, 0, 0, DateTimeZone.UTC)
    val intervalEnd = new DateTime(endTs) //new DateTime(1990, 2, 1, 0, 0, 0, DateTimeZone.UTC)
    val interval = new Interval(intervalStart, intervalEnd)
    val inRdds: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = ContextRDD(inputs(i.id)
      .groupBy {
        case (k, r) => {
          val time = k.time
          new DateTime(interval.getStartMillis + (time.toInstant.toEpochMilli - interval.getStartMillis) / intervalDuration.getMillis * intervalDuration.getMillis, DateTimeZone.UTC)
        }
      }
      .flatMap { case (intervalKey, tiles) =>
        val t = tiles.map {
          case (k, t) => {
            (SpaceTimeKey(k.spatialKey, TemporalKey(intervalKey.toInstant.getMillis)), t)
          }
        }
        t
      }.reduceByKey(
      (t1: MultibandTile, t2: MultibandTile) => {
        var tils: Array[Tile] = Array()
        for (i <- 0 until t1.bandCount) {
          tils = tils :+ t1.band(i).convert(CellType.fromName("float32"))
            .combineDouble(t2.band(i).convert(CellType.fromName("float32"))) {
              (v1, v2) => {
                (v1 + v2) / 2
              }
            }
        }
        ArrayMultibandTile(tils)
      }
    ), m)

    inRdds

  }


}
