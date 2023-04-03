
import { MapContainer, TileLayer, WMSTileLayer, useMap, GeoJSON, useMapEvent, Popup, Marker } from 'react-leaflet';
import { connect, useDispatch } from "react-redux";
import L from 'leaflet';
import React, { useEffect, useState } from "react";
// import {TileLayerWMS} from "leaflet/src/layer/tile/TileLayer.WMS";

// import {addLayer, addSearchLayer, changeMapView} from "../actions";
import { BasemapLayer, FeatureLayer } from "react-esri-leaflet";

import RasterLayer from "./RasterLayer";
import { addLayer, addSearchLayer, changeMapView, changeMapZoom, mapViewWasSet, updateChartData } from "../actions";
import { generateId } from "../utils/helpers";

const mapStateToProps = state => {
    return {
        map: state.MapReducer
    }
};

function ChangeMapZoom({ zoom }) {
    const map = useMap();
    map.setZoom(zoom);
    return null;
}

function ChangeMapView({ zoom, center, changeMapView }) {
    const map = useMap();
    if (changeMapView) {
        map.setView(center, zoom);
    }
    return null;
}

function MapViewChangeEvent() {
    const dispatch = useDispatch();
    const map = useMapEvent('moveend', () => {
        dispatch(mapViewWasSet(map.getCenter(), map.getZoom()))
    })
    return null
}

const EsriFeatureLayerQuery = {
    point: function pointGeomQueryEsriFeatureLayer(featureLayerUrl, point, success, failure) {
        let data = {
            "where": "1=1",
            "geometry": point.join(','),
            "geometryType": "esriGeometryPoint",
            "inSR": "4326",
            "spatialRel": "esriSpatialRelIntersects",
            "resultType": "none",
            "distance": "0.0",
            "units": "esriSRUnit_Meter",
            "returnGeodetic": "false",
            "outFields": "*",
            "returnGeometry": "true",
            "returnCentroid": "false",
            "featureEncoding": "esriDefault",
            "multipatchOption": "xyFootprint",
            "applyVCSProjection": "false",
            "returnIdsOnly": "false",
            "returnUniqueIdsOnly": "false",
            "returnCountOnly": "false",
            "returnExtentOnly": "false",
            "returnQueryGeometry": "false",
            "returnDistinctValues": "false",
            "cacheHint": "false",
            "returnZ": "false",
            "returnM": "false",
            "returnExceededLimitFeatures": "true",
            "sqlFormat": "none",
            "f": "geojson",
            // "resultRecordCount": 1
        };
        let formData = new FormData();
        Object.keys(data).map(k => {
            formData.append(k, data[k]);
        })
        fetch(featureLayerUrl, {
            method: 'POST',
            body: formData
        }).then(r => r.json()).then(result => {
            return success(result.features);
        })
            .catch(e => {
                console.log(e);
                return failure(e);
            });
    },
    id: function pointGeomQueryEsriFeatureLayer(featureLayerUrl, id, success, failure) {
        let data = {
            "where": "id=" + id,
            "geometry": '',
            "geometryType": "esriGeometryPoint",
            "inSR": "4326",
            "spatialRel": "esriSpatialRelIntersects",
            "resultType": "none",
            "distance": "0.0",
            "units": "esriSRUnit_Meter",
            "returnGeodetic": "false",
            "outFields": "*",
            "returnGeometry": "true",
            "returnCentroid": "false",
            "featureEncoding": "esriDefault",
            "multipatchOption": "xyFootprint",
            "applyVCSProjection": "false",
            "returnIdsOnly": "false",
            "returnUniqueIdsOnly": "false",
            "returnCountOnly": "false",
            "returnExtentOnly": "false",
            "returnQueryGeometry": "false",
            "returnDistinctValues": "false",
            "cacheHint": "false",
            "returnZ": "false",
            "returnM": "false",
            "returnExceededLimitFeatures": "true",
            "sqlFormat": "none",
            "f": "json",
            // "resultRecordCount": 1
        };
        let formData = new FormData();
        Object.keys(data).map(k => {
            formData.append(k, data[k]);
        })
        fetch(featureLayerUrl, {
            method: 'POST',
            body: formData
        }).then(r => r.json()).then(result => {
            return success(result.features);
        })
            .catch(e => {
                console.log(e);
                return failure(e);
            });
    }
}

const MapLayers = (props) => {
    let layers = Object.keys(props.map.layers).map(layerId => props.map.layers[layerId]);
    layers = layers.sort((a, b) => { return a.sortOrder - b.sortOrder });

    return <div>
        {
            layers.map(layer => {
                if (!layer.active)
                    return '';
                switch (layer.type) {
                    case 'WMS':
                        return props.map.zoom >= layer.minZoom ? <WMSTileLayer
                            maxZoom={24} key={layer.id} url={layer.url} params={{
                                layers: layer.id,
                                format: 'image/png',
                                transparent: true,
                            }} /> : ''
                    case 'VECTOR':
                        return <GeoJSON attribution="&copy; credits due..." data={layer.data} key={layer.id} />
                    case 'RASTER':
                        return <RasterLayer key={layer.id} data={layer.data} options={{
                            interpolation: 'nearestNeighbour',
                            samplingRatio: 4,
                            imageSmoothing: false,
                            debug: false
                        }} />
                    case 'TILE':
                        return <TileLayer key={layer.id}
                            url={layer.url}
                            maxZoom={24}
                        />
                    case 'ESRI_FEATURE':
                        return props.map.zoom > 16 ? (
                            <FeatureLayer
                                key={layer.id}
                                url={layer.url}
                                // where={"status = 'LIVE'"}
                                // where={"spatial_extents_shared = 'F'"}
                                style={() => {
                                    return {
                                        color: "#888",
                                        weight: 0.5,
                                        fillOpacity: 0
                                    }
                                }}
                                pointToLayer={(geojson, latlng) => {
                                    return L.marker(latlng, {
                                        icon: L.divIcon({
                                            // iconUrl: 'https://esri.github.io/esri-leaflet/img/earthquake-icon.png',
                                            iconSize: null,
                                            html: '<div>' + geojson.properties['full_address_number'] + '</div>'
                                            // iconAnchor: [13.5, 17.5],
                                            // popupAnchor: [0, -11]
                                        })
                                    });
                                }}
                            />
                        ) : '';
                    case 'VECTOR_GJ':
                        return true ? (
                            <GeoJSON data={layer.data} key={layer.id} />
                        ) : '';
                }
                return '';
            })
        }
    </div>
}


function ClickEvent(props) {
    // return;
    const dispatch = useDispatch();
    const map = useMapEvent('click', (e) => {
        let fromTs = null, toTs = null;
        props.setMarkerPos(e.latlng)
        if (props.timeIndexes && props.timeIndexes.length > 0) {
            fromTs = props.timeIndexes[0]['ts']
            toTs = props.timeIndexes[props.timeIndexes.length - 1]['ts']
        } else {
            return;
        }
        fetch(`http://localhost:8082/getPixelAt?sensorName=Landsat&fromTs=${fromTs}&toTs=${toTs}&y=${e.latlng.lat}&x=${e.latlng.lng}`)
            .then(r => r.json())
            .then(r => {
                dispatch(updateChartData({
                    type: 'line',
                    labels: props.timeIndexes.map(p => new Date(p['ts']).toLocaleDateString()),
                    values: r.data.map(b => b[4] - b[3])
                }))
                console.log(r.data.map(b => b[4] - b[3]))
            })
            .catch(e => {
                console.log(e)
            })
    })
    return null
}

const Map = (props) => {
    const dispatch = useDispatch()
    const [markerPos, setMarkerPos] = useState(null)
    useEffect(() => {
        if (!props.map.tIndex) {
            return;
        }
        dispatch(addLayer({
            type: 'TILE',
            id: 'data',
            active: true,
            url: `http://localhost:8082/tile/Landsat/{z}/{x}/{y}.png?tIndex=${props.map.tIndex}&bands=${props.map.bands.join(",")}&vmin=0&vmax=${props.map.vizMaxValue}`,
            name: 'Streets - Base',
            sortOrder: 0,
            showLegend: false,
            showInLayerList: false
        }))
    }, [props.map.tIndex, props.map.bands])

    // console.log(props.map.searchLayer)
    return (
        <MapContainer maxZoom={24} style={{ height: 'calc(100vh - 120px)', width: '100%' }} center={props.map.center} zoom={props.map.zoom}>
            {/*<ChangeMapZoom zoom={props.map.zoom} />*/}

            {/*<LayerHandler layers={props.map.layers} />*/}

            <MapViewChangeEvent />
            <ClickEvent timeIndexes={props.map.timeIndexes} tIndex={props.map.tIndex} setMarkerPos={setMarkerPos} />

            <ChangeMapView
                center={props.map.mapView.center}
                zoom={props.map.mapView.zoom}
                changeMapView={props.map.changeMapView}
            />
            <MapLayers map={props.map} />
            {
                props.map.searchLayer ? (
                    <GeoJSON data={props.map.searchLayer.data} />
                ) : ''
            }

            {markerPos ? (<Marker position={markerPos}>
                {/* <Popup>
                    
                </Popup> */}
            </Marker>) : ''}
        </MapContainer>
    );
}

export default connect(mapStateToProps)(Map);