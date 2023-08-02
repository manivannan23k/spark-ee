package com.gishorizon

import geotrellis.layer.{FloatingLayoutScheme, LayoutDefinition, Metadata, SpaceTimeKey, SpatialKey, TemporalProjectedExtent, TileLayerMetadata}
import geotrellis.proj4.CRS
import geotrellis.raster.{MultibandTile, Tile, TileLayout}
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.spark.store.RasterReader
import geotrellis.spark.store.file.FileLayerWriter
import geotrellis.spark.{CollectTileLayerMetadata, ContextRDD, MultibandTileLayerRDD, RasterSourceRDD, withTileRDDMergeMethods, withTilerMethods}
import geotrellis.spark.store.hadoop.HadoopGeoTiffRDD
import geotrellis.store.LayerId
import geotrellis.store.file.FileAttributeStore
import geotrellis.store.index.ZCurveKeyIndexMethod
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

import java.time.{ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter

object RddUtils {

  def getMultiTiledRDDWithMeta(sc: SparkContext, inputFile: String, tileSize: Int): RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = {
//    val geoTiff = GeoTiffRasterSource(inputFile)

    val layer: RDD[(ProjectedExtent, MultibandTile)] = HadoopGeoTiffRDD[ProjectedExtent, ProjectedExtent, MultibandTile](
      path = new Path(inputFile),
      uriToKey = {
        case (uri, projectedExtent) =>
          projectedExtent
      },
      options = HadoopGeoTiffRDD.Options.DEFAULT
    )(sc, RasterReader.multibandGeoTiffReader)
    val (zoom, meta) = CollectTileLayerMetadata.fromRDD[ProjectedExtent, MultibandTile, SpatialKey](layer, FloatingLayoutScheme(tileSize))
    val tiled: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = ContextRDD(
      layer.tileToLayout(meta.cellType, meta.layout),
      meta
    )
    tiled
  }

  def getMultiTiledTemporalRDDWithMeta(sc: SparkContext, inputFiles: String, tileSize: Int, dt: ZonedDateTime): RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = {

    var extent: Extent = null
    var crs: CRS = null
    print(DateTime.now() + "------READ ALL 10 TIFFs-----")
//    sc.parallelize(inputFiles).map{
//      case (inputFile) => {
//        val gt: MultibandGeoTiff = GeoTiffReader.readMultiband(inputFile)
//      }
//    }
//
    for(i <- 0 to 100){
      val gt: MultibandGeoTiff = GeoTiffReader.readMultiband(inputFiles)
      crs = gt.crs
      if(extent == null){
        extent = gt.extent
      }
      else{
        extent = extent.combine(gt.extent)
      }
      println(i)
    }
    val pe = ProjectedExtent(extent, crs)
    print(DateTime.now() + "------DONE READ ALL 10 TIFFs-----")



    val layer: RDD[(TemporalProjectedExtent, MultibandTile)] = HadoopGeoTiffRDD[ProjectedExtent, TemporalProjectedExtent, MultibandTile](
      path = new Path(inputFiles),
      uriToKey = {
        case (uri, projectedExtent) =>
          TemporalProjectedExtent(projectedExtent, dt)
      },
      options = HadoopGeoTiffRDD.Options.DEFAULT
    )(sc, RasterReader.multibandGeoTiffReader)
    val (zoom, meta) = CollectTileLayerMetadata.fromRDD[TemporalProjectedExtent, MultibandTile, SpaceTimeKey](layer, FloatingLayoutScheme(tileSize))
    val tiled: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = ContextRDD(
      layer.tileToLayout(meta.cellType, meta.layout),
      meta
    )
    tiled
  }

  /***
   * Each band as a time step of 10 days - 36 bands (last 6 duplicate bands are ignored)
   * Converts MultibandTile to Tile
   * @param sc
   * @param inputFile
   * @return
   */
  def singleTiffTimeSeriesRdd(sc: SparkContext, inputFile: String): (RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], Int) = {
    val _sRdd = HadoopGeoTiffRDD[ProjectedExtent, TemporalProjectedExtent, MultibandTile](
      path = new Path(inputFile),
      uriToKey = {
        case (uri, pExtent) =>
          val year = uri.toString.split("/").last.replace("Test", "").replace(".tif", "").split("-").head
          //          println(f"$year-01-01T00:00:00Z")
          TemporalProjectedExtent(pExtent, ZonedDateTime.parse(f"$year-01-01T00:00:00Z", DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.ofHoursMinutes(5, 30))))
      },
      options = HadoopGeoTiffRDD.Options.DEFAULT
    )(sc, RasterReader.multibandGeoTiffReader)
    val (zoom, meta) = CollectTileLayerMetadata.fromRDD[TemporalProjectedExtent, MultibandTile, SpaceTimeKey](_sRdd, FloatingLayoutScheme(10))
    val _oRdd: RDD[(SpaceTimeKey, MultibandTile)] = _sRdd.tileToLayout(meta.cellType, meta.layout)
    (
      ContextRDD(
        _oRdd.mapPartitions(
          p =>
            p.flatMap(mt => {
              mt._2.bands.toList.take(36).zipWithIndex.map(t => {
                val dt = new DateTime(mt._1.instant).plusDays(t._2 * 10)
                (new SpaceTimeKey(mt._1.col, mt._1.row, dt.getMillis), t._1)
              })
            })
        ),
        meta
      ),
      zoom
    )
  }

  /** *
   * Each band as a time step of 10 days - 36 bands (last 6 duplicate bands are ignored)
   * Converts MultibandTile to Tile
   *
   * @param sc
   * @param inputFile
   * @return
   */
  def singleTiffTimeSeriesRdd(sc: SparkContext, inputFile: String, tileSize: Int): (RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], Int) = {
    val _sRdd = HadoopGeoTiffRDD[ProjectedExtent, TemporalProjectedExtent, MultibandTile](
      path = new Path(inputFile),
      uriToKey = {
        case (uri, pExtent) =>
          val year = uri.toString.split("/").last.replace("Test", "").replace(".tif", "").split("-").head
          //          println(f"$year-01-01T00:00:00Z")
          TemporalProjectedExtent(pExtent, ZonedDateTime.parse(f"$year-01-01T00:00:00Z", DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.ofHoursMinutes(5, 30))))
      },
      options = HadoopGeoTiffRDD.Options.DEFAULT
    )(sc, RasterReader.multibandGeoTiffReader)
    var _tileSizeX = tileSize
    var _tileSizeY = tileSize
    if(tileSize==0){
      val rasterSource = GeoTiffReader.readMultiband(inputFile)
      _tileSizeX = rasterSource.tile.cols
      _tileSizeY = rasterSource.tile.rows
    }
    val (zoom, meta) = CollectTileLayerMetadata.fromRDD[TemporalProjectedExtent, MultibandTile, SpaceTimeKey](_sRdd, FloatingLayoutScheme(_tileSizeX, _tileSizeY))
    val _oRdd: RDD[(SpaceTimeKey, MultibandTile)] = _sRdd.tileToLayout(meta.cellType, meta.layout)
    (
      ContextRDD(
        _oRdd.mapPartitions(
          p =>
            p.flatMap(mt => {
              mt._2.bands.toList.take(36).zipWithIndex.map(t => {
                val dt = new DateTime(mt._1.instant).plusDays(t._2 * 10)
                (new SpaceTimeKey(mt._1.col, mt._1.row, dt.getMillis), t._1)
              })
            })
        ),
        meta
      ),
      zoom
    )
  }

  def mergeRdds(outputData: Array[RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]]): RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = {
    var meta = outputData(0).metadata
    outputData.foreach {
      o => {
        meta = meta.merge(o.metadata)
      }
    }
    val ratio = Math.round(((meta.extent.xmax - meta.extent.xmin) / (meta.extent.ymax - meta.extent.ymin)) / (((outputData(0).metadata.extent.xmax - outputData(0).metadata.extent.xmin)) / ((outputData(0).metadata.extent.ymax - outputData(0).metadata.extent.ymin))))
    val xTileSize = 256 * (outputData.length * ratio)
    val yTileSize = 256 * (outputData.length / ratio)
    meta = TileLayerMetadata(meta.cellType, new LayoutDefinition(meta.layout.extent, new TileLayout(1, 1, yTileSize.toInt, xTileSize.toInt)), meta.extent, meta.crs, meta.bounds)
    println(meta)
    val outProj: Array[RDD[(ProjectedExtent, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]] = outputData.map(o => {
      ContextRDD(
        o.map {
          case (k, t) => {
            (ProjectedExtent(o.metadata.mapTransform(k), o.metadata.crs), t)
          }
        }, o.metadata
      )
    })
    var out: RDD[(ProjectedExtent, MultibandTile)] = outProj(0)
    outProj.foreach {
      o => {
        out = out.merge(o)
      }
    }
    val result = ContextRDD(out, meta)

    val (zoom, newmeta) = CollectTileLayerMetadata.fromRDD[ProjectedExtent, MultibandTile, SpatialKey](result, FloatingLayoutScheme(256))
    ContextRDD(result.tileToLayout(newmeta.cellType, newmeta.layout), newmeta)

  }

  def mergeTemporalRdds(outputData: Array[RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]]): RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = {
    var meta = outputData(0).metadata
    outputData.foreach {
      o => {
        meta = meta.merge(o.metadata)
      }
    }
    val xTileSize = (meta.layout.extent.xmax-meta.layout.extent.xmin)/outputData(0).metadata.layout.cellwidth //256 * (outputData.length * ratio)
    val yTileSize = (meta.layout.extent.ymax-meta.layout.extent.ymin)/outputData(0).metadata.layout.cellwidth //256 * (outputData.length / ratio)
    meta = TileLayerMetadata(
      meta.cellType,
      new LayoutDefinition(
        meta.layout.extent,
        new TileLayout(
          Math.ceil((xTileSize)/256).toInt,
          Math.ceil((yTileSize)/256).toInt,
          256,
          256
        )
      ),
      meta.extent, meta.crs, meta.bounds
    )

    val tmp = ContextRDD(outputData.map {
      rd => {
        rd.map {
          case (k, v) => {
            val e = k.spatialKey.extent(rd.metadata.layout)
            val fe = meta.layout.extent
            val c: Int = Math.ceil((fe.ymax - e.ymax) / meta.layout.cellSize.height / 256).toInt
            val r: Int = Math.ceil((e.xmin-fe.xmin) / meta.layout.cellSize.width / 256).toInt
            (SpaceTimeKey(SpatialKey(r, c), k.temporalKey), v)
          }
        }
      }
    }.reduce {
      (a, b) => {
        a.merge(b)
      }
    }, meta)
    println(meta)
    tmp.checkpoint()
    tmp
//    val outProj: Array[RDD[(TemporalProjectedExtent, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]] = outputData.map(o => {
//      ContextRDD(
//        o.map {
//          case (k, t) => {
//            (TemporalProjectedExtent(o.metadata.mapTransform(k), o.metadata.crs, k.instant), t)
//          }
//        }, o.metadata
//      )
//    })
//    var out: RDD[(TemporalProjectedExtent, MultibandTile)] = outProj(0)
//    outProj.foreach {
//      o => {
//        out = out.merge(o)
//      }
//    }
//    val result = ContextRDD(out, meta)
//
//    val (zoom, newmeta) = CollectTileLayerMetadata.fromRDD[TemporalProjectedExtent, MultibandTile, SpaceTimeKey](result, FloatingLayoutScheme(256))
//    ContextRDD(result.tileToLayout(newmeta.cellType, newmeta.layout), newmeta)
//
//    //    result.tileToLayout(
//    //      meta
//    //    )
  }

  def mosaicSTRddToSingle(): Unit = {

  }
}
