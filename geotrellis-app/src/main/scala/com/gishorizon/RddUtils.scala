package com.gishorizon

import geotrellis.layer.{FloatingLayoutScheme, LayoutDefinition, Metadata, SpaceTimeKey, SpatialKey, TemporalProjectedExtent, TileLayerMetadata}
import geotrellis.raster.{MultibandTile, Tile, TileLayout}
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.spark.store.file.FileLayerWriter
import geotrellis.spark.{CollectTileLayerMetadata, ContextRDD, MultibandTileLayerRDD, RasterSourceRDD, withTileRDDMergeMethods, withTilerMethods}
import geotrellis.spark.store.hadoop.HadoopGeoTiffRDD
import geotrellis.store.LayerId
import geotrellis.store.file.FileAttributeStore
import geotrellis.store.index.ZCurveKeyIndexMethod
import geotrellis.vector.ProjectedExtent
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

import java.time.{ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter

object RddUtils {
  def getMultiTiledRDD(implicit sc: SparkContext, inputFile: String, tileSize: Int): (RDD[(SpatialKey, MultibandTile)], TileLayerMetadata[SpatialKey]) = {
    val layer: RDD[(ProjectedExtent, MultibandTile)] = HadoopGeoTiffRDD[ProjectedExtent, ProjectedExtent, MultibandTile](
      path = new Path(inputFile),
      uriToKey = {
        case (uri, projectedExtent) =>
          projectedExtent
      },
      options = HadoopGeoTiffRDD.Options.DEFAULT
    )
    val (zoom, meta) = CollectTileLayerMetadata.fromRDD[ProjectedExtent, MultibandTile, SpatialKey](layer, FloatingLayoutScheme(tileSize))
    val tiled: RDD[(SpatialKey, MultibandTile)] = layer.tileToLayout(meta.cellType, meta.layout)
    (tiled, meta)
  }

  def getMultiTiledRDDWithMeta(implicit sc: SparkContext, inputFile: String, tileSize: Int): RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = {
    val layer: RDD[(ProjectedExtent, MultibandTile)] = HadoopGeoTiffRDD[ProjectedExtent, ProjectedExtent, MultibandTile](
      path = new Path(inputFile),
      uriToKey = {
        case (uri, projectedExtent) =>
          projectedExtent
      },
      options = HadoopGeoTiffRDD.Options.DEFAULT
    )
    val (zoom, meta) = CollectTileLayerMetadata.fromRDD[ProjectedExtent, MultibandTile, SpatialKey](layer, FloatingLayoutScheme(tileSize))
    val tiled: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = ContextRDD(
      layer.tileToLayout(meta.cellType, meta.layout),
      meta
    )
    tiled
  }

  def getTiledRDD(implicit sc: SparkContext, inputFile: String, tileSize: Int): (RDD[(SpatialKey, Tile)], TileLayerMetadata[SpatialKey]) = {
    val layer: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD[ProjectedExtent, ProjectedExtent, Tile](
      path = new Path(inputFile),
      uriToKey = {
        case (uri, projectedExtent) =>
          projectedExtent
      },
      options = HadoopGeoTiffRDD.Options.DEFAULT
    )
    val (zoom, meta) = CollectTileLayerMetadata.fromRDD[ProjectedExtent, Tile, SpatialKey](layer, FloatingLayoutScheme(tileSize))
    val tiled: RDD[(SpatialKey, Tile)] = layer.tileToLayout(meta.cellType, meta.layout)
    (tiled, meta)
  }

  def getMultiRowRdd(implicit sc: SparkContext, inputFile: String): RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = {
    val rasterSource = GeoTiffReader.readMultiband(inputFile)
    val rowRdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = RasterSourceRDD.spatial(
      GeoTiffRasterSource(inputFile),
      LayoutDefinition(
        rasterSource.extent,
        TileLayout(1, rasterSource.tile.rows, rasterSource.tile.cols, 1)
      )
    )
    rowRdd
  }

  def getRowRdd(implicit sc: SparkContext, inputFile: String): RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = {
    val rasterSource = GeoTiffReader.readSingleband(inputFile)
    val rowRdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = RasterSourceRDD.spatial(
      GeoTiffRasterSource(inputFile),
      LayoutDefinition(
        rasterSource.extent,
        TileLayout(1, rasterSource.tile.rows, rasterSource.tile.cols, 1)
      )
    )
    ContextRDD(rowRdd.map {
      case (k, mt) => {
        (k, mt.band(0))
      }
    }, rowRdd.metadata)
  }

  def getMultiColRdd(implicit sc: SparkContext, inputFile: String): RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = {
    val rasterSource = GeoTiffReader.readMultiband(inputFile)
    val rowRdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = RasterSourceRDD.spatial(
      GeoTiffRasterSource(inputFile),
      LayoutDefinition(
        rasterSource.extent,
        TileLayout(rasterSource.tile.cols, 1, 1, rasterSource.tile.rows)
      )
    )
    rowRdd
  }

  def getColRdd(implicit sc: SparkContext, inputFile: String): RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = {
    val rasterSource = GeoTiffReader.readMultiband(inputFile)
    val rowRdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = RasterSourceRDD.spatial(
      GeoTiffRasterSource(inputFile),
      LayoutDefinition(
        rasterSource.extent,
        TileLayout(rasterSource.tile.cols, 1, 1, rasterSource.tile.rows)
      )
    )
    ContextRDD(rowRdd.map {
      case (k, mt) => {
        (k, mt.band(0))
      }
    }, rowRdd.metadata)
  }

  /***
   * Each band as a time step of 10 days - 36 bands (last 6 duplicate bands are ignored)
   * Converts MultibandTile to Tile
   * @param sc
   * @param inputFile
   * @return
   */
  def singleTiffTimeSeriesRdd(implicit sc: SparkContext, inputFile: String): (RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], Int) = {
    val _sRdd = HadoopGeoTiffRDD[ProjectedExtent, TemporalProjectedExtent, MultibandTile](
      path = new Path(inputFile),
      uriToKey = {
        case (uri, pExtent) =>
          val year = uri.toString.split("/").last.replace("Test", "").replace(".tif", "").split("-").head
          //          println(f"$year-01-01T00:00:00Z")
          TemporalProjectedExtent(pExtent, ZonedDateTime.parse(f"$year-01-01T00:00:00Z", DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.ofHoursMinutes(5, 30))))
      },
      options = HadoopGeoTiffRDD.Options.DEFAULT
    )
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
  def singleTiffTimeSeriesRdd(implicit sc: SparkContext, inputFile: String, tileSize: Int): (RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], Int) = {
    val _sRdd = HadoopGeoTiffRDD[ProjectedExtent, TemporalProjectedExtent, MultibandTile](
      path = new Path(inputFile),
      uriToKey = {
        case (uri, pExtent) =>
          val year = uri.toString.split("/").last.replace("Test", "").replace(".tif", "").split("-").head
          //          println(f"$year-01-01T00:00:00Z")
          TemporalProjectedExtent(pExtent, ZonedDateTime.parse(f"$year-01-01T00:00:00Z", DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.ofHoursMinutes(5, 30))))
      },
      options = HadoopGeoTiffRDD.Options.DEFAULT
    )
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

  /***
   * Each tiff file as Time step of 1 year
   * Returns RDD(SpaceTimeKey, MultibandTile)
   * @param sc
   * @param inputPath
   * @return
   */
  def multiTiffTimeSeriesRdd(implicit sc: SparkContext, inputPath: String): (RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], Int) = {
    val _rdd = HadoopGeoTiffRDD[ProjectedExtent, TemporalProjectedExtent, MultibandTile](
      path = new Path(inputPath),
      uriToKey = {
        case (uri, pExtent) => {
          val year = uri.toString.split("/").last.replace("Test", "").replace(".tif", "").split("-").head
          TemporalProjectedExtent(pExtent, ZonedDateTime.parse(f"$year-01-01T00:00:00Z", DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.ofHoursMinutes(5, 30))))
        }
      },
      options = HadoopGeoTiffRDD.Options.DEFAULT
    )
    val (zoom, meta) = CollectTileLayerMetadata.fromRDD[TemporalProjectedExtent, MultibandTile, SpaceTimeKey](_rdd, FloatingLayoutScheme(10))
    val _oRdd: RDD[(SpaceTimeKey, MultibandTile)] = _rdd.tileToLayout(meta.cellType, meta.layout)
    (
      ContextRDD(
        _oRdd,
        meta
      ),
      zoom
    )
  }

  /***
   * Each band as a time step of 10 days for multiple tiff files
   * @param sc
   * @param inputPath
   * @return
   */
  def multiTiffCombinedTimeSeriesRdd(implicit sc: SparkContext, inputPath: String): (RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]], Int) = {
    val (_rdd, zoom) = multiTiffTimeSeriesRdd(sc, inputPath)
    (ContextRDD(
      _rdd.mapPartitions(
        p =>
          p.flatMap(mt => {
            mt._2.bands.toList.take(36).zipWithIndex.map(t => {
              val dt = new DateTime(mt._1.instant).plusDays(t._2 * 10)
              (new SpaceTimeKey(mt._1.col, mt._1.row, dt.getMillis), t._1)
            })
          })
      ),
      _rdd.metadata
    ), zoom)
  }

  def saveMultiBandTimeSeriesRdd(
               rdd: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]],
               zoom: Int,
               layerName: String,
               timeStepInDays: Int,
               catalogPath: String
             ): Unit = {
    val attributeStore = FileAttributeStore(catalogPath)
    val writer = FileLayerWriter(attributeStore)
    writer.write(LayerId(layerName, zoom), rdd, ZCurveKeyIndexMethod.byDays(timeStepInDays))
    println(f"Saved $layerName")
  }

  def saveSingleBandTimeSeriesRdd(
                                  rdd: RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]],
                                  zoom: Int,
                                  layerName: String,
                                  timeStepInDays: Int,
                                  catalogPath: String
                                ): Unit = {
    val attributeStore = FileAttributeStore(catalogPath)
    val writer = FileLayerWriter(attributeStore)
    writer.write(LayerId(layerName, zoom), rdd, ZCurveKeyIndexMethod.byDays(timeStepInDays))
    println(f"Saved $layerName")
  }

  def saveMultiBandRdd(
                        rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]],
                        zoom: Int,
                        layerName: String,
                        catalogPath: String
                      ): Unit = {
    val attributeStore = FileAttributeStore(catalogPath)
    val writer = FileLayerWriter(attributeStore)
    writer.write(LayerId(layerName, zoom), rdd, ZCurveKeyIndexMethod)
    println(f"Saved $layerName")
  }

  def saveSingleBandRdd(
                        rdd: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]],
                        zoom: Int,
                        layerName: String,
                        catalogPath: String
                      ): Unit = {
    val attributeStore = FileAttributeStore(catalogPath)
    val writer = FileLayerWriter(attributeStore)
    writer.write(LayerId(layerName, zoom), rdd, ZCurveKeyIndexMethod)
    println(f"Saved $layerName")
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

//    result.tileToLayout(
//      meta
//    )
  }
}
