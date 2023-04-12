import * as AppConfig from '../utils/AppConfig'

const initialState = {
    // center: [-36.902306, 174.696037],
    center: [25.71315688761512, 
        90.12840270996094],
    zoom: 13,
    layers: AppConfig.layers,
    mapView: {
        center: [25.71315688761512, 
            90.12840270996094],
        zoom: 13
    },
    changeMapView: false,
    searchLayer: null,
    timeIndexes: null,
    dataVizMode: 'rgb',
    bands: [4, 3, 2],
    tIndex: null,
    vizMaxValue: 0.25,
    chartData: null
};

const MapReducer = (state = initialState, action) => {
    let layers = state.layers;
    switch (action.type) {
        case "CHANGE_ZOOM":
            return {
                ...state,
                zoom: action.payload
            };
        case "CHANGE_MAP_VIEW":
            return {
                ...state,
                zoom: action.payload.zoom,
                center: action.payload.center,
                mapView: {
                    zoom: action.payload.zoom,
                    center: action.payload.center
                },
                changeMapView: true
            };
        case "MAP_VIEW_WAS_SET":
            return {
                ...state,
                zoom: action.payload.zoom,
                center: action.payload.center,
                mapView: {
                    zoom: action.payload.zoom,
                    center: action.payload.center
                },
                changeMapView: false
            };
        case "UPDATE_CHART_DATA":
            return {
                ...state,
                chartData: action.payload
            }
        case "UPDATE_RGB_BANDS":
            return {
                ...state,
                bands: [...action.payload.bands],
                vizMaxValue: action.payload.vizMaxValue
            }
        case "TOGGLE_LAYER_STATE":
            layers[action.payload.layerId].active = action.payload.active;
            return {
                ...state,
                layers: layers
            }
        case "SET_TIME_INDEXEX":
            return {
                ...state,
                timeIndexes: action.payload
            }
        case "UPDATE_TINDEX":
            return {
                ...state,
                tIndex: action.payload
            }
        case "ADD_LAYER":
            layers[action.payload.id] = action.payload;
            return {
                ...state,
                layers: layers
            }
        case "ADD_SEARCH_LAYER":
            if(Boolean(state.searchLayer) && Boolean(layers[state.searchLayer])){
                delete layers[state.searchLayer];
            }
            return {
                ...state,
                searchLayer: action.payload,
                layers: {...layers}
            }
        default:
            return {
                ...state
            };
    }
};

export default MapReducer;