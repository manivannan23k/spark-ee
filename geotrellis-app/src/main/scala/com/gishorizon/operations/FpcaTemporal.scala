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

object FpcaTemporal {

    def runProcess(inputs: Map[String, RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]], processOperation: ProcessOperation)
    : RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]
    = {
      val intervalDays = 10
      val intervalDuration = Duration.standardDays(intervalDays)

      var inRdds: Array[RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]] = Array()
      var meta: TileLayerMetadata[SpaceTimeKey] = null

      for (inp <- processOperation.inputs.indices){
        val i = processOperation.inputs(inp)
        val m = inputs(i.id).metadata
        meta = m
        val intervalStart = new DateTime(m.bounds.get.minKey.time.toInstant.toEpochMilli) //new DateTime(1989, 11, 1, 0, 0, 0, DateTimeZone.UTC)
        val intervalEnd = new DateTime(m.bounds.get.maxKey.time.toInstant.toEpochMilli) //new DateTime(1990, 2, 1, 0, 0, 0, DateTimeZone.UTC)
        println("Interval Start:", intervalStart)
        println("Interval End:", intervalEnd)
        val interval = new Interval(intervalStart, intervalEnd)
        val t: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = ContextRDD(inputs(i.id)
          .groupBy {
            case (k, r) => {
              val time = k.time
              new DateTime(interval.getStartMillis + (time.toInstant.toEpochMilli - interval.getStartMillis) / intervalDuration.getMillis * intervalDuration.getMillis, DateTimeZone.UTC)
            }
          }
          .flatMap { case (intervalKey, tiles) =>
            val t = tiles.map {
              case (k, t) => {
                println("Interval Key:", intervalKey)
                (SpaceTimeKey(k.spatialKey, TemporalKey(intervalKey.toInstant.getMillis)), t)
              }
            }
            t
          }.reduceByKey(
          (t1: MultibandTile, t2: MultibandTile) => {
            var tils: Array[Tile] = Array()
            for (i <- 0 until t1.bandCount) {
              tils = tils :+ t1.band(i)
                .combineDouble(t2.band(i)) {
                  (v1, v2) => {
                    if(v1==0 && v2==0){
                      0.0
                    }else if(v1==0){
                      v2
                    }else if(v2==0){
                      v1
                    }else{
                      (v1 + v2) / 2
                    }
                  }
                }
            }
            ArrayMultibandTile(tils)
          }
        )
          .map {
            case (k, v) => {
              val t: MultibandTile = ArrayMultibandTile(v.band(0))
              (k.spatialKey, t)
            }
          }
          .reduceByKey(
            (t1: MultibandTile, t2: MultibandTile) => {
              var bds1: Array[Tile] = Array()
              for(i <- t1.bands){
                bds1 = bds1 :+ i
              }
              for (i <- t2.bands) {
                bds1 = bds1 :+ i
              }
              val t: MultibandTile = ArrayMultibandTile(bds1)
              t
            }
          ), TileLayerMetadata(m.cellType, m.layout, m.extent, m.crs, m.bounds.asInstanceOf[Bounds[SpatialKey]]))
        inRdds = inRdds :+ t
      }
      val outRdd = inRdds.reduce {
        (rdd1, rdd2) => {
          val rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = ContextRDD(rdd1.++(rdd2), rdd1.metadata)
          rdd
        }
      }
        .map {
          case (k, mt) => {
            var _d = Array[Array[Double]]()
            var _d1 = Array[Double]()
            mt.foreachDouble((v: Array[Double]) => {
              _d1 = Array.concat(_d1, v)
            })
            for (i <- 0 until mt.cols * mt.rows) {
              var _pixel = Array[Double]()
              for (_time <- mt.bands.indices) {
                _pixel = _pixel :+ _d1((_time) + (i * mt.bands.indices.length))
              }
              _d = _d :+ _pixel
            }
            (k, _d)
          }
        }


      val t: RDD[(SpatialKey, Array[Array[IndexedRow]])] = outRdd.aggregateByKey(
        outRdd.first()._2.map {
          _ =>
            val _t: Array[Array[Double]] = Array[Array[Double]]()
            _t
        }
      )(
        {
          (l, t) => {
            var _t_ = Array[Array[Array[Double]]]()
            for (i <- l.indices) {
              val _t1 = l(i)
              val _t2 = t(i)
              val _t3: Array[Array[Double]] = _t1 :+ _t2
              _t_ = _t_ :+ _t3
            }
            _t_
          }
        },
        {
          (l1, l2) => {
            var _t = Array[Array[Array[Double]]]()
            for (i <- l2.indices) {
              val _t1 = l1(i)
              val _t2 = l2(i)
              val _t3 = Array.concat(_t1, _t2)
              _t = _t :+ _t3
            }
            _t
          }
        }
      ).map {
        case (k, v) => {
          var _d = Array[Array[IndexedRow]]()
          for (i <- v.indices) {
            var data = Array[IndexedRow]()
            for (j <- v(i).indices) {
              val ir = IndexedRow(j, Vectors.dense(v(i)(j)))
              data = data :+ ir
            }
            _d = _d :+ data
          }
          (k, _d)
        }
      }


      val _t: RDD[(SpaceTimeKey, MultibandTile)] = t
        .map {
          case (k, v) =>
            val o: Array[Array[Double]] = v.map {
              __v => {
                val (r, _) = FpcaDev.normalCompute(__v)
//                val _a = r.toLocalMatrix().toArray
                if(r == null){
                  (0 until __v(0).vector.size).map(_ =>0.0).toArray[Double]
                }else{
                  val _a = r.toArray
                  var min = 99.0
                  var max = -99.0
                  _a.foreach {
                    va =>
                      if (min > va) {
                        min = va
                      }
                      if (va > max) {
                        max = va
                      }
                  }
                  _a.map {
                    ___v => {
                      if (___v.isNaN) {
                        ___v
                      } else {
                        (255 * (___v - min) / (max - min)).toInt
                      }
                    }
                  }
                }
              }
            }
            var _o = Array[Array[Double]]()
            for (i <- o.indices) {
              val _t = Array[Double]()
              for (j <- o(i).indices) {
                if (_o.length <= j) {
                  _o = _o :+ _t
                }
                _o(j) = _o(j) :+ o(i)(j)
              }
            }

            (k, MultibandTile(
              _o.map {
                _v => {
                  var min = 99.0
                  var max = -99.0
                  _v.foreach {
                    v =>
                      if (min > v) {
                        min = v
                      }
                      if (v > max) {
                        max = v
                      }
                  }
                  val at: Tile = ArrayTile(_v.map {
                    ___v => {
                      if (___v.isNaN) {
                        ___v
                      } else {
                        (255 * (___v - min) / (max - min)).toInt
                      }
                    }
                  }, 256, 256)
                  at
                }
              }
            ))
        }
        .map{
          case (k, t) => {
            (SpaceTimeKey(k, meta.bounds.get.maxKey.time), t)
          }
        }
    val rdd: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = ContextRDD(_t, meta)
//      val rdd: MultibandTileLayerRDD[SpatialKey] with Metadata[TileLayerMetadata[SpatialKey]] = ContextRDD(_t, inRdds(0).metadata)
//      write(rdd, "data/out/fpcafull.tiff")
//      println("Done")
      rdd
    }
//  def main(args: Array[String]): Unit = {
//    implicit val sc: SparkContext = Spark.context
//
//    val paths = Array(
//      "data/NDVISampleTest/Test1998-99.tif",
//      "data/NDVISampleTest/Test1999-00.tif",
//      "data/NDVISampleTest/Test2000-01.tif",
//      "data/NDVISampleTest/Test2001-02.tif",
//    )
//
//    val inRdds: Array[RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]] = paths.map(p => {
//      val (rdd, meta) = RddUtils.getMultiTiledRDD(sc, p, 20)
//      val _r: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = ContextRDD(rdd, meta)
//      _r
//    })
//
//    val outRdd = inRdds.reduce {
//      (rdd1, rdd2) => {
//        val rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = ContextRDD(rdd1.++(rdd2), rdd1.metadata)
//        rdd
//      }
//    }
//      .map {
//      case (k, mt) => {
//        var _d = Array[Array[Double]]()
//        var _d1 = Array[Double]()
//        mt.foreachDouble((v: Array[Double]) => {
//          _d1 = Array.concat(_d1, v)
//        })
//        for (i <- 0 until mt.cols * mt.rows) {
//          var _pixel = Array[Double]()
//          for (_time <- mt.bands.indices) {
//            _pixel = _pixel :+ _d1((_time) + (i * mt.bands.indices.length))
//          }
//          _d = _d :+ _pixel
//        }
//        (k, _d)
//      }
//    }
//
//
//    val t: RDD[(SpatialKey, Array[Array[IndexedRow]])] = outRdd.aggregateByKey(
//      outRdd.first()._2.map {
//        _ =>
//          val _t: Array[Array[Double]] = Array[Array[Double]]()
//          _t
//      }
//    )(
//      {
//        (l, t) => {
//          var _t_ = Array[Array[Array[Double]]]()
//          for (i <- l.indices) {
//            val _t1 = l(i)
//            val _t2 = t(i)
//            val _t3: Array[Array[Double]] = _t1 :+ _t2
//            _t_ = _t_ :+ _t3
//          }
//          _t_
//        }
//      },
//      {
//        (l1, l2) => {
//          var _t = Array[Array[Array[Double]]]()
//          for (i <- l2.indices) {
//            val _t1 = l1(i)
//            val _t2 = l2(i)
//            val _t3 = Array.concat(_t1, _t2)
//            _t = _t :+ _t3
//          }
//          _t
//        }
//      }
//    ).map {
//        case (k, v) => {
//          var _d = Array[Array[IndexedRow]]()
//          for (i <- v.indices) {
//            var data = Array[IndexedRow]()
//            for (j <- v(i).indices) {
//              val ir = IndexedRow(j, Vectors.dense(v(i)(j)))
//              data = data :+ ir
//            }
//            _d = _d :+ data
//          }
//          (k, _d)
//        }
//      }
//
//
//    val _t: RDD[(SpatialKey, MultibandTile)] = t
//      .map {
//        case (k, v) =>
//          val o: Array[Array[Double]] = v.map {
//            __v => {
//              FpcaDev.normalCompute(__v)
//              val (r, _) = FpcaDev.normalCompute(__v)
//              val _a = r.toArray
//              var min = 99.0
//              var max = -99.0
//              _a.foreach {
//                va =>
//                  if (min > va) {
//                    min = va
//                  }
//                  if (va > max) {
//                    max = va
//                  }
//              }
//              _a.map {
//                ___v => {
//                  if (___v.isNaN) {
//                    ___v
//                  } else {
//                    (255 * (___v - min) / (max - min)).toInt
//                  }
//                }
//              }
//            }
//          }
//          var _o = Array[Array[Double]]()
//          for (i <- o.indices) {
//            val _t = Array[Double]()
//            for (j <- o(i).indices) {
//              if (_o.length <= j) {
//                _o = _o :+ _t
//              }
//              _o(j) = _o(j) :+ o(i)(j)
//            }
//          }
//
//          (k, MultibandTile(
//            _o.map {
//              _v => {
//                var min = 99.0
//                var max = -99.0
//                _v.foreach {
//                  v =>
//                    if (min > v) {
//                      min = v
//                    }
//                    if (v > max) {
//                      max = v
//                    }
//                }
//                val at: Tile = ArrayTile(_v.map {
//                  ___v => {
//                    if (___v.isNaN) {
//                      ___v
//                    } else {
//                      (255 * (___v - min) / (max - min)).toInt
//                    }
//                  }
//                }, 20, 20)
//                at
//              }
//            }
//          ))
//      }
//
//    val rdd: MultibandTileLayerRDD[SpatialKey] with Metadata[TileLayerMetadata[SpatialKey]] = ContextRDD(_t, inRdds(0).metadata)
//    write(rdd, "data/out/fpcafull.tiff")
//    println("Done")
//  }

  def write(tiles: MultibandTileLayerRDD[SpatialKey], path: String): Unit = {
    GeoTiff(tiles.stitch().tile, tiles.metadata.extent, tiles.metadata.crs).write(path)
  }

}
