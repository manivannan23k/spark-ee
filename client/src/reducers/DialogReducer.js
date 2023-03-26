const initialState = {
    showAddLayerDialog: false,
    showVectorToolDialog: false,
    currentVectorTool: 'buffer',
    showRasterToolDialog: false,
    currentRasterTool: 'raster_calculator'
};

const DialogReducer = (state = initialState, action) => {
    switch (action.type) {
        case "TOGGLE_ADD_LAYER":
            return {
                ...state,
                showAddLayerDialog: action.payload.flag
            };
        case 'TOGGLE_VECTOR_TOOL':
            return {
                ...state,
                showVectorToolDialog: action.payload.flag,
                currentVectorTool: action.payload.tool
            };
        case 'TOGGLE_RASTER_TOOL':
            return {
                ...state,
                showRasterToolDialog: action.payload.flag,
                currentRasterTool: action.payload.tool
            };
        default:
            return {
                ...state
            };
    }
};

export default DialogReducer;