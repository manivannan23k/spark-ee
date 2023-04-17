const initialState = {
    showAddLayerDialog: false,
    showModelBuilderDialog: false,
    showQueryResultsDialog: false
};

const DialogReducer = (state = initialState, action) => {
    switch (action.type) {
        case "TOGGLE_ADD_LAYER":
            return {
                ...state,
                showAddLayerDialog: action.payload
            };
        case 'TOGGLE_MODEL_BUILDER':
            return {
                ...state,
                showModelBuilderDialog: action.payload
            };
        case 'TOGGLE_QUERY_RESULTS_DIALOG':
            return {
                ...state,
                showQueryResultsDialog: action.payload
            };
        default:
            return {
                ...state
            };
    }
};

export default DialogReducer;