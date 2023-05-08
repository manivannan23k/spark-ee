import { connect, useDispatch } from "react-redux";
import AppModal from "./AppModal";
import { addLayer, toggleQueryResultsDialog } from "../actions";

const mapStateToProps = (state) => {
    return {
        map: state.MapReducer,
        dialog: state.DialogReducer
    }
}

const QueryResults = (props) => {
    const dispatch = useDispatch();
    if (props.map.queryResults.length <= 0) {
        return <></>;
    }
    let firstResult = props.map.queryResults[0];
    return <>
        <AppModal btnText={""} flag={props.dialog.showQueryResultsDialog} setFlag={(flag) => {
            dispatch(toggleQueryResultsDialog(flag))
        }}
            content=
            <div>
                <ul>
                    {
                        props.map.queryResults.map(r => {
                            return <li>{r.dsName} - {new Date(r.ts).toLocaleString('in')}</li>
                        })
                    }
                </ul>
                <button onClick={() => {
                    let lId = (Math.random() + 1).toString(36).substring(6);
                    let layer = {
                        type: 'DATA_TILE',
                        group: 'RASTER_DATA',
                        id: lId,
                        active: true,
                        tIndex: firstResult.tIndex,
                        tIndexes: props.map.queryResults.map(qr => qr.tIndex),
                        aoiCode: firstResult['aoiCode'],
                        dsId: firstResult['dsName'],
                        noOfBands: firstResult['dsData']['no_of_bands'],
                        name: 'Layer: ' + lId,
                        sortOrder: 0,
                        showLegend: false,
                        showInLayerList: true,
                        style: {
                            min: 0,
                            max: 1,
                            bands: [4, 3, 2],
                            type: "rgb"
                        }
                    }
                    dispatch(addLayer(layer));
                    dispatch(toggleQueryResultsDialog(false))
                }}>Add</button>
            </div>
        />

    </>
}

export default connect(mapStateToProps)(QueryResults);