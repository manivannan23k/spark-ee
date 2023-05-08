package com.gishorizon.operations

import geotrellis.layer.{SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.macros.{DoubleTileMapper, DoubleTileVisitor, IntTileMapper, IntTileVisitor}
import geotrellis.raster.{ArrayTile, CellType, MutableArrayTile, Tile}
import geotrellis.spark.store.file.FileLayerReader
import geotrellis.store.{Intersects, LayerId}
import geotrellis.store.file.FileAttributeStore
import com.gishorizon.RddUtils.singleTiffTimeSeriesRdd
import com.gishorizon.{RddUtils, Spark}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{DenseMatrix, Vectors}
import org.apache.spark.mllib.linalg.distributed._

import java.awt.Color
import java.awt.image.BufferedImage
import java.io.File
import javax.imageio.ImageIO

object FpcaTile {

//  def main(args: Array[String]): Unit = {
//    implicit val sc: SparkContext = Spark.context
//
////    val outputCatalogPath = "data/out/multiTiffCombinedTsRdd"
////    val layerName = "multiTiffCombinedTsRdd"
////    val attributeStore = FileAttributeStore(outputCatalogPath)
////
////    val reader = FileLayerReader(attributeStore)
////    val queryResult = reader
////      .query[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](LayerId(layerName, 0))
////      .where(
////        Intersects(
////          new SpatialKey(0, 0).extent(attributeStore.readMetadata[TileLayerMetadata[SpaceTimeKey]](LayerId(layerName, 0)))
////        )
////      ).result
//
//    val (rdd, _) = singleTiffTimeSeriesRdd(sc, "data/NDVISampleTest/Test1998-99.tif", 0)
//    val _ks = rdd.keys.collect()
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
//
//  }

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
