//References :https://github.com/geotrellis/geotrellis-spark-job.g8
//References :https://geotrellis.github.io/geotrellis-workshop/docs/spark-layer-rdd
//Torun on sbt-shell put
//test:runMain com.gishorizon.Main --inputs /Users/ManiChan/Geotrellis/TimeAnalysis/data/LE07_L2SP_143042_20050203_20200915_02_T1_SR_B2.TIF --output file:///Users/ManiChan/Geotrellis/TimeAnalysis/data/LE07_L2SP_143042_20050203_20200915_02_T1_SR_B2.TIF.cat --numPartitions 3
//test:runMain com.gishorizon.Main --inputs data/NDVISampleTest/Test1998-99.tif --output file:///Users/ashutosh/Geotrellis/TimeAnalysis/data/LE07_L2SP_143042_20050203_20200915_02_T1_SR_B2.TIF.cat --numPartitions 3

package com.gishorizon

import cats.implicits._
import com.monovore.decline._
import geotrellis.layer._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.gdal._
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.store.file.{FileLayerManager, FileLayerReader, FileLayerWriter, FileRDDWriter}
import geotrellis.spark.store.hadoop.HadoopGeoTiffRDD
import geotrellis.spark.{CollectTileLayerMetadata, withTilerMethods}
import geotrellis.store.LayerId
import geotrellis.store.file.{FileAttributeStore, FileCollectionLayerReader}
import geotrellis.store.index.ZCurveKeyIndexMethod
import geotrellis.vector._
import org.apache.hadoop.fs.Path
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.joda.time.DateTime
import org.log4s._

import java.time.{ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter

//import geotrellis.raster.io.geotiff.GeoTiffReader
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.reader.GeoTiffReader

object  Main {
  @transient private[this] lazy val logger = getLogger

  private val inputsOpt = Opts.options[String](
    "inputs", help = "The path that points to data that will be read")
  private val outputOpt = Opts.option[String](
    "output", help = "The path of the output tiffs")
  private val partitionsOpt =  Opts.option[Int](
    "numPartitions", help = "The number of partitions to use").orNone

  private val command = Command(name = "geotrellis-spark-job", header = "GeoTrellis App: geotrellis-spark-job") {
    (inputsOpt, outputOpt, partitionsOpt).tupled
  }

//  def main(args: Array[String]): Unit = {
//    command.parse(args, sys.env) match {
//      case Left(help) =>
//        System.err.println(help)
//        sys.exit(1)
//
//      case Right((i, o, p)) =>
//        try {
//          run(i.toList, o, p)(Spark.context)
//        } finally {
//          Spark.session.stop()
//        }
//    }
//  }
//
//  def run(inputs: List[String], output: String, numPartitions: Option[Int])(implicit sc: SparkContext): Unit = {
//
////    println("----------Creating RDDs----------")
////    val colRdd = RddUtils.getColRdd(sc, inputs.head)
////    val rowRdd = RddUtils.getColRdd(sc, inputs.head)
////
////    val (singleTiffTsRdd, z1): (RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], Int) = RddUtils.singleTiffTimeSeriesRdd(sc, inputs.head)
////    val (multiTiffTsRdd, z2): (RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], Int) = RddUtils.multiTiffTimeSeriesRdd(sc, "data/NDVISampleTest/*.tif")
//    val (multiTiffCombinedTsRdd, z3): (RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], Int) = RddUtils.multiTiffCombinedTimeSeriesRdd(sc, "data/NDVISampleTest/*.tif")
////
////    println("----------RDDs are Created----------")
////
////    println("----------Ingesting RDDs----------")
////    RddUtils.saveMultiBandRdd(
////      colRdd, 1, "colRdd", "data/output"
////    )
////    RddUtils.saveMultiBandRdd(
////      rowRdd, 1, "rowRdd", "data/output"
////    )
////    RddUtils.saveSingleBandTimeSeriesRdd(
////      singleTiffTsRdd, z1, "singleTiffTsRdd", 10, "data/output"
////    )
////    RddUtils.saveMultiBandTimeSeriesRdd(
////      multiTiffTsRdd, z2, "multiTiffTsRdd", 365, "data/output"
////    )
//    RddUtils.saveSingleBandTimeSeriesRdd(
//      multiTiffCombinedTsRdd, z3, "multiTiffCombinedTsRdd", 10, "data/output"
//    )
////    println("----------Ingestion Completed----------")
//
//
//
//  }
}
