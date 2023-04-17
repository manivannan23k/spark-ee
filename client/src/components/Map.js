
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

                    case 'DATA_TILE':
                        return <TileLayer key={layer.id}
                            url={`http://localhost:8082/tile/${layer.dsId}/{z}/{x}/{y}.png?tIndex=${layer.tIndex}&bands=5,4,3&vmin=0&vmax=1500&aoi_code=${layer.aoiCode}`}
                            //http://localhost:8082/tile/Landsat_OLI/{z}/{x}/{y}.png?tIndex=913543204&bands=4,3,2&vmin=0&vmax=20000&aoi_code=qwertyuiopasdfgh
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
        console.log(e.latlng)
        // let fromTs = null, toTs = null;
        // props.setMarkerPos(e.latlng)
        // if (props.timeIndexes && props.timeIndexes.length > 0) {
        //     fromTs = props.timeIndexes[0]['ts']
        //     toTs = props.timeIndexes[props.timeIndexes.length - 1]['ts']
        // } else {
        //     return;
        // }
        // fetch(`http://localhost:8082/getPixelAt?sensorName=Landsat&fromTs=${fromTs}&toTs=${toTs}&y=${e.latlng.lat}&x=${e.latlng.lng}`)
        //     .then(r => r.json())
        //     .then(r => {
        //         dispatch(updateChartData({
        //             type: 'line',
        //             labels: props.timeIndexes.map(p => new Date(p['ts']).toLocaleDateString()),
        //             values: r.data.map(b => b[4] - b[3])
        //         }))
        //         console.log(r.data.map(b => b[4] - b[3]))
        //     })
        //     .catch(e => {
        //         console.log(e)
        //     })
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
        // dispatch(addLayer({
        //     type: 'TILE',
        //     id: 'data',
        //     active: true,
        //     url: `http://localhost:8082/tile/Landsat_OLI/{z}/{x}/{y}.png?tIndex=${props.map.tIndex}&bands=${props.map.bands.join(",")}&vmin=0&vmax=${props.map.vizMaxValue}`,
        //     name: 'Streets - Base',
        //     sortOrder: 0,
        //     showLegend: false,
        //     showInLayerList: false
        // }))
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