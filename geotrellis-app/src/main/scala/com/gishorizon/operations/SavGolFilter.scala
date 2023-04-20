package com.gishorizon.operations

import breeze.linalg.{DenseMatrix, inv}
import com.gishorizon.RddUtils._
import com.gishorizon.Spark
import geotrellis.layer.{Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.raster.io.geotiff.{GeoTiff, GeoTiffOptions}
import geotrellis.raster.{ArrayMultibandTile, ArrayTile, MultibandTile, Raster, Tile}
import geotrellis.spark.ContextRDD
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import geotrellis.raster.io.geotiff.writer.GeoTiffWriter
import geotrellis.raster._
import geotrellis.spark._

object SavGolFilter {

//  def main(args: Array[String]): Unit = {
//    try {
//      run(Spark.context)
//    } finally {
//      Spark.session.stop()
//    }
//  }

  def runProcess(inputs: Map[String, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]], operation: ProcessOperation): RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = {
    var outRdds: Array[RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]] = Array()
    var in1: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = inputs(operation.inputs(0).id)
    val rdd = in1
    runForRdd(rdd, rdd.metadata)
  }

  def runForRdd(rdd: RDD[(SpatialKey, MultibandTile)], meta: TileLayerMetadata[SpatialKey]): RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = {
    val tileSize = 5
//    val (rdd, meta) = getMultiTiledRDD(sc, "data/NDVISampleTest/Test1998-99.tif", tileSize)
    //TODO: Recalculate SavGol Based on Score
    val result: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = ContextRDD(rdd.map {
      case (k, t) => {
        var tv = Array[Array[Int]]()
        t.foreach {
          (v: Array[Int]) => {
            tv = tv :+ v
          }
        }
        val bandCount = t.bandCount
        var resultTiles = Array[Tile]()
        for (b <- 0 until bandCount) {
          resultTiles = resultTiles :+ ArrayTile(tv.map {
            v => {
              //TODO: Calculate least sq error and apply optimum params: Least square error from original values: p = 2-4, w = 3-5
              var wParam = 3
              var pParam = 2
              var err = Double.MaxValue //1000.0

              var _a = v

              var outAr = Array[Double]()

              for(w <- 3 to 5){
                for(p <- 2 to 4){
                  if(w>p){
                    val rY = solve(_a.map(e => e.toDouble), w, p)//.map(e=>e.toInt)
                    val _err = getLeaseSquareError(_a.map(e=>e.toInt), rY)
                    if(_err<err){
                      wParam = w
                      pParam = p
                      err = _err
                      outAr = rY
                    }
                  }
                }
              }


              outAr
            }
          }.map {
            v => {
              v(b)
            }
          }, tileSize, tileSize).rotate270.flipVertical
        }
        val mTile: MultibandTile = new ArrayMultibandTile(resultTiles)
        (
          k, mTile
        )
      }
    }, meta)
    result
//    println("------------Saving----------")
//    val rasterTile: Raster[MultibandTile] =  result.stitch()
//    GeoTiffWriter.write(GeoTiff(rasterTile, meta.crs), "data/result.tif")
//    println("------------Done----------")
  }

  def getLeaseSquareError(x: Array[Double], y: Array[Double]): Double = {
    var er = 0.0
    for(i <- y.indices){
      er = er + Math.pow(y(i)-x(i), 2)
    }
    er
  }

  def getX(w: Int, p: Int, values: Array[Double]): DenseMatrix[Double] = {
    var matVals = Array[Double]()
    for(i <- 0 to p){
      for(j <- 0 until w){
        matVals = matVals :+ Math.pow(values(j), i)
      }
    }
    val X = new DenseMatrix(w, p+1, matVals)
    X
  }

  def getHFromX(X: DenseMatrix[Double]): DenseMatrix[Double] = {
    inv( X.t * X ) * X.t
  }

  def getCoeff(H: DenseMatrix[Double], values: Array[Double], w: Int, p: Int): Array[Double] = {
    val Y = new DenseMatrix(w, 1, values)
    (H * Y).toArray
  }

  def solve(y: Array[Double], w: Int, p: Int): Array[Double] = {
    val hw: Int = w/2
    val xVals = y.indices.map(e => e.toDouble).toArray
    var outY: Array[Double] = y.slice(0, hw)
    for (i <- 0 until y.length - 2*hw){
      val _y = y.slice(i, i + w)
      val X = getX(w, p, xVals.slice(i, i + w))
      val H = getHFromX(X)
      val coeff = getCoeff(H, _y, w, p)
      var yCalc: Double = 0
      for (c <- 0 to p) {
        val _x = xVals(i+hw)
        yCalc = yCalc + coeff(c) * Math.pow(_x, c)
      }
      outY = outY :+ yCalc
    }
    outY  = outY ++ y.slice(y.length-hw, y.length)
//    println(outY.mkString("Array(", ", ", ")"))
    outY
  }

}
