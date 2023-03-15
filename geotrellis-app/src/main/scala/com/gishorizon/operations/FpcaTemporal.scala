package com.gishorizon.operations

import geotrellis.layer.{Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.raster.io.geotiff.GeoTiff
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
import geotrellis.layer.{Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.raster.io.geotiff._
import geotrellis.raster.{io => _, _}
import geotrellis.spark.stitch._

import java.awt.Color
import java.awt.image.BufferedImage
import java.io.File
import java.time.{ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter
import javax.imageio.ImageIO
import scala.Double.NaN

object FpcaTemporal {

  def main(args: Array[String]): Unit = {
    implicit val sc: SparkContext = Spark.context

    val outputCatalogPath = "data/out/multiTiffTsRdd"
    val layerName = "multiTiffTsRdd"
    val attributeStore = FileAttributeStore(outputCatalogPath)

    val reader = FileLayerReader(attributeStore)
    val data = reader
      .query[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]](LayerId(layerName, 0))
      .where(
        Intersects(
          new SpatialKey(0, 0).extent(attributeStore.readMetadata[TileLayerMetadata[SpaceTimeKey]](LayerId(layerName, 0)))
        )
      )
      .result

    val meta = data.metadata
//    val data = ContextRDD(_data.filter {
//      case (k, v) => {
//        k.spatialKey == new SpatialKey(1, 1)
//      }
//    }, meta)

    println("------------Data Read----------")

//    val (data, z) = RddUtils.multiTiffTimeSeriesRdd(sc, "data/NDVISampleTest/*.tif")
//    RddUtils.saveMultiBandTimeSeriesRdd(data, z, "multiTiffTsRdd", 1, "data/out/multiTiffTsRdd")

//    val mtRdd = queryResult.map{
//      case (k, t)=>{
//        val _k = new SpaceTimeKey(
//          k.col,
//          k.row,
//          ZonedDateTime.parse(f"${k.time.getYear}-01-01T00:00:00Z", DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.ofHoursMinutes(5, 30))).toInstant.toEpochMilli
//        )
//        (_k, t)
//      }
//    }
//      .aggregateByKey(MultibandTile(queryResult.first()._2.map {
//        case _ =>
//          0
//      }))({
//        (l, t) => {
//          MultibandTile(
//            Array.concat(l.bands.toArray[Tile], Array(t))
//          )
//        }
//      }, {
//        (t1, t2) => {
//          MultibandTile(Array.concat(t1.bands.toArray[Tile], t2.bands.toArray[Tile]))
//        }
//      })
      val outRdd = data.map{
        case (k, mt) => {
          var _d = Array[Array[Double]]()
          var _d1 = Array[Double]()
          mt.foreachDouble((v:Array[Double])=>{
            _d1 = Array.concat(_d1, v)
          })
          for (i <- 0 until mt.cols*mt.rows){
            var _pixel = Array[Double]()
            for (_time <- mt.bands.indices){
              _pixel = _pixel :+ _d1((_time) + (i * mt.bands.indices.length)) //mt.cols*mt.rows *
            }
            _d = _d :+ _pixel
          }
          (k.spatialKey, _d)
        }
      }
      val t = outRdd.aggregateByKey(
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
          (l1, l2)=>{
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
      )
        .map{
          case (k, v) => {
            var _d = Array[Array[IndexedRow]]()
            for (i <- v.indices){
              var data = Array[IndexedRow]()
              for (j <- v(i).indices){
                val ir = IndexedRow(j, Vectors.dense(v(i)(j)))
                data = data :+ ir
              }
              _d = _d :+ data
            }
            (k, _d)
          }
        }
    val _t = t.collect()
      .map {
      case (k, v) =>
        val r = v.map {
          case (_v) => {
            sc.parallelize(_v)
          }
        }
        val o: Array[Array[Double]] = r.map{
          __v=>{
            val (r, _) = FPCA.run(sc, new IndexedRowMatrix(__v).toBlockMatrix().cache())
            val _a = r.toLocalMatrix().toArray
            var min = 99.0
            var max = -99.0

            _a.foreach{
              va=>
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
        var _o = Array[Array[Double]]()
        for (i <- o.indices) {
          val _t = Array[Double]()
          for (j <- o(i).indices){
            if(_o.length<=j){
              _o = _o :+ _t
            }
            _o(j) = _o(j) :+ o(i)(j)
          }
        }
        val t = ZonedDateTime.parse(f"${2021}-01-01T00:00:00Z", DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.ofHoursMinutes(5, 30))).toInstant.toEpochMilli
        (k, MultibandTile(
          _o.map {
            _v => {
              //            val r = attributeStore.readMetadata[TileLayerMetadata[SpaceTimeKey]](layerName, 0).rows
              var min = 99.0
              var max = -99.0
              _v.foreach{
                v=>
                  if(min>v){
                    min = v
                  }
                  if (v > max) {
                    max = v
                  }
              }
              val at: Tile = ArrayTile(_v.map {
                ___v => {
                  if(___v.isNaN){
                    ___v
                  }else{
                    (255 * (___v - min) / (max - min)).toInt
                  }
                }
              }, 27, 28)
              at
            }
          }
        ))
      }

    val rdd: MultibandTileLayerRDD[SpatialKey] = ContextRDD(sc.parallelize(_t), meta.asInstanceOf[TileLayerMetadata[SpatialKey]])
//    val _r = MultibandTileLayerRDD(rdd = rdd, metadata = meta)

    write(rdd, "data/out/fpcafull.tiff")


//    _t._2.map{
//      case (v)=>{
//        v
//      }
//    }
//: Array[(SpaceTimeKey, Array[Array[Double]])]
////    val (rdd, _) = singleTiffTimeSeriesRdd(sc, "data/NDVISampleTest/Test1998-99.tif", 0)
    val _ks = t.keys.collect()
//    val maxT = _ks.map(m=>m.instant).max
//    val minT = _ks.map(m=>m.instant).min
//    val indexedRowRDD = rdd.map {
//      case (key, tile) =>
//        (key, Vectors.dense(tile.toArray().map(a => (a.toDouble))))
//    }.map {
//      case (key, vectors) =>
////        IndexedRow(((1356652800000L - key.instant) / 1000L * 60 * 60 * 24 * 10).toInt, vectors)
//        IndexedRow(((maxT - key.instant) / (1000L * 60 * 60 * 24 * 10)).toInt, vectors)
//    }
//    val res = FPCA.run(sc, new IndexedRowMatrix(indexedRowRDD).toBlockMatrix().cache())
//    val data = res._1.toLocalMatrix().toArray
//    val at: Tile = ArrayTile(data, rdd.metadata.cols.toInt ,rdd.metadata.rows.toInt)
//    val image: BufferedImage = new BufferedImage(rdd.metadata.cols.toInt ,rdd.metadata.rows.toInt, BufferedImage.TYPE_INT_RGB)
//    for (y <- 0 until image.getHeight) {
//      for (x <- 0 until image.getWidth) {
//        val v = 255 * (data(y * image.getWidth + x) - data.min) / (data.max - data.min)
//        image.setRGB(x, y, new Color(v.toInt, v.toInt, v.toInt, v.toInt).getRGB)
//      }
//    }
//    val out = new File("data/out/pca.png")
//    ImageIO.write(image, "png", out)
    println("Done")

  }

  def write(tiles: MultibandTileLayerRDD[SpatialKey], path: String): Unit = {
    GeoTiff(tiles.stitch().tile, tiles.metadata.extent, tiles.metadata.crs).write(path)
  }

//  def run(implicit sc: SparkContext): Unit = {
//    val rowRdd = RddUtils.getRowRdd(sc, "data/NDVISampleTest/Test1998-99.tif")
//    val colRdd = RddUtils.getColRdd(sc, "data/NDVISampleTest/Test1998-99.tif")
//
//
////    val _t1 = rowRdd.map{
////      case (rk, row) => {
////        (
////          rk, colRdd.filter{
////            case (key, tile) => {
////              key == rk
////            }
////          }
////          .first()._2
////        )
////      }
////    }
//
//
//
//    val T = 100
//    val N = 4
//    val L = 3
//    val rand = new scala.util.Random
//
//    var data = Array[IndexedRow]()
//
//    for (i <- 0 until N){
//      val v = Vectors.dense(
//        List.fill(T)(0).map {
//          i => {
//            rand.nextFloat().toDouble
//          }
//        }.toArray[Double]
//      )
//      data = data :+ IndexedRow(i, v)
//    }
//
//    val scData = sc.parallelize(data)
//    val matrix = new IndexedRowMatrix(scData).toBlockMatrix().cache()
//
//    //FPCA function
//    val _N = matrix.numRows().toInt
//    val _T = matrix.numCols().toInt
//    val _t: List[Int] = (0 until _N).map{i=>{i.toInt}}.toList
//    val scale = new CoordinateMatrix(
//      sc.parallelize(_t)
//        .map{
//          i => {
//            MatrixEntry(i, i, 1/Math.sqrt(_T))
//          }
//        },
//      _N,
//      _N
//    ).toBlockMatrix()
//    val svd = scale.multiply(matrix).transpose.toIndexedRowMatrix().computeSVD(L)
//    val s = svd.s
//    val Va = svd.V
//    var Vl = Array[IndexedRow]()
//
//    for (i <- 0 until N){
//      Vl = Vl :+ IndexedRow(i, Vectors.dense(
//        Va.toArray(L * i + 0),
//        Va.toArray(L * i + 1),
//        Va.toArray(L * i + 2)
//      ))
//    }
//    val V = new IndexedRowMatrix(sc.parallelize(Vl), N, L)
//    val S = new DenseMatrix(L, L, org.apache.spark.mllib.linalg.Matrices.diag(s).toArray)
//    val Si = new DenseMatrix(L, L, org.apache.spark.mllib.linalg.Matrices.diag(
//      Vectors.dense(s.toArray.map(e=>1/e))
//    ).toArray)
//    val scores = V.multiply(S)
//    val t = V.multiply(Si).toBlockMatrix()
//    val mt = matrix.transpose
//    val components= mt.multiply(t)
//    val FPCA = components.multiply(scores.toBlockMatrix().transpose )
//    println(FPCA)
//  }
}
