export const toggleLayerState = (layerId, active) => {
    return {
        type: 'TOGGLE_LAYER_STATE',
        payload: {layerId, active}
    }
};
export const changeMapZoom = (zoom) => {
    return {
        type: 'CHANGE_ZOOM',
        payload: zoom
    }
}
export const changeMapView = (center, zoom) => {
    return {
        type: 'CHANGE_MAP_VIEW',
        payload: {center, zoom}
    }
}
export const mapViewWasSet = (center, zoom) => {
    return {
        type: 'MAP_VIEW_WAS_SET',
        payload: {center, zoom}
    }
}
export const addLayer = (layer) => {
    return {
        type: 'ADD_LAYER',
        payload: layer
    }
};
export const addSearchLayer = (layer) => {
    return {
        type: 'ADD_SEARCH_LAYER',
        payload: layer
    }
};

export const setTimeIndexes = (data) => {
    return {
        type: 'SET_TIME_INDEXEX',
        payload: data
    }
};

export const updateTimeIndex = (data) => {
    return {
        type: "UPDATE_TINDEX",
        payload: data
    }
}

export const updateRGBBands = (data) => {
    return {
        type: "UPDATE_RGB_BANDS",
        payload: data
    }
}

export const updateChartData = (data) => {
    return {
        type: "UPDATE_CHART_DATA",
        payload: data
    }
}
export const toggleAddLayerDialog = (flag) => {
    return {
        type: 'TOGGLE_ADD_LAYER',
        payload: flag
    }
};
export const toggleModelBuilderDialog = (flag) => {
    return {
        type: 'TOGGLE_MODEL_BUILDER',
        payload: flag
    }
};