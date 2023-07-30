package com.gishorizon.operations

import geotrellis.layer.{Bounds, Metadata, SpaceTimeKey, SpatialKey, TemporalKey, TileLayerMetadata}
import geotrellis.raster.{ArrayTile, MultibandTile, Raster, Tile}
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD, RasterSourceRDD, RasterSummary, TileLayerRDD, withTilerMethods, _}
import geotrellis.spark.store.file.FileLayerReader
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.store.{Intersects, LayerId}
import geotrellis.store.file.FileAttributeStore
import com.gishorizon.RddUtils.singleTiffTimeSeriesRdd
import com.gishorizon.{Logger, RddUtils, Spark}
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

      var meta: TileLayerMetadata[SpaceTimeKey] = null
      var irdds: Array[RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]] = Array()
      for(i <- processOperation.inputs.indices){
        val inrdd:  RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = inputs(processOperation.inputs(i).id)
        irdds = irdds :+ inrdd
        meta = inrdd.metadata
      }


      val outRdd = irdds.reduce {
        (rdd1, rdd2) => {
          val rdd: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = ContextRDD(rdd1.++(rdd2), rdd1.metadata)
          rdd
        }
      }.map {
        case (k, v) => {
          (k.spatialKey, v)
        }
      }
        .reduceByKey(
          (t1: MultibandTile, t2: MultibandTile) => {
            Logger.log("Merging tiles by key...")
            var bds1: Array[Tile] = Array()
            for (i <- t1.bands) {
              bds1 = bds1 :+ i
            }
            for (i <- t2.bands) {
              bds1 = bds1 :+ i
            }
            val t: MultibandTile = ArrayMultibandTile(bds1)
            t
          }
        )
        .map {
          case (k, mt) => {
            var _d = Array[Array[Double]]()
            mt.foreachDouble((v: Array[Double]) => {
              _d = _d :+ v
            })
            println("[APP_LOG] " + DateTime.now() + "Tile to double...")
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
          println("[APP_LOG] " + DateTime.now() + "Tile to IRow...")
          (k, _d)
        }
      }


      val _t: RDD[(SpaceTimeKey, MultibandTile)] = t
        .map {
          case (k, v) =>
            println("[APP_LOG] " + DateTime.now() + "Running FPCA")
            val o: Array[Array[Double]] = v.map {
              __v => {
                if(__v.forall(p =>
                  p.vector.toArray.forall(_p => !_p.isNaN)
                )){
                  val (r, comps) = FpcaDev.normalCompute(__v)
                  if (r == null) {
                    (0 until __v(0).vector.size).map(_ => 0.0).toArray[Double]
                  } else {
                    val _a = r.toArray
                    _a
//                    var min = 99.0
//                    var max = -99.0
//                    _a.foreach {
//                      va =>
//                        if (min > va) {
//                          min = va
//                        }
//                        if (va > max) {
//                          max = va
//                        }
//                    }
//                    _a.map {
//                      ___v => {
//                        if (___v.isNaN) {
//                          ___v
//                        } else {
//                          (255 * (___v - min) / (max - min)).toInt
//                        }
//                      }
//                    }
                  }
                }else{
                  (0 until __v(0).vector.size).map(_ =>0.0).toArray[Double]
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

            println("[APP_LOG] " + DateTime.now() + "FPCA tile generation...")
            (k, MultibandTile(
              _o.map {
                _v => {
//                  var min = 99.0
//                  var max = -99.0
//                  _v.foreach {
//                    v =>
//                      if (min > v) {
//                        min = v
//                      }
//                      if (v > max) {
//                        max = v
//                      }
//                  }
                  val at: Tile = ArrayTile(_v.map {
                    ___v => {
                      ___v
//                      if (___v.isNaN) {
//                        ___v
//                      } else {
//                        (255 * (___v - min) / (max - min)).toInt
//                      }
                    }
                  }, 256, 256)
                  at.rotate270.flipVertical
                }
              }
            ))
        }
        .map{
          case (k, t) => {
            println("[APP_LOG] " + DateTime.now() + "FPCA Done")
            (SpaceTimeKey(k, meta.bounds.get.maxKey.time), t)
          }
        }
    val rdd: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = ContextRDD(_t, meta)
      rdd
    }
  def write(tiles: MultibandTileLayerRDD[SpatialKey], path: String): Unit = {
    GeoTiff(tiles.stitch().tile, tiles.metadata.extent, tiles.metadata.crs).write(path)
  }

}
