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
                    /***
                     * {
                        type: 'DATA_TILE',
                        id: 'data',
                        active: true,
                        tIndex: 913543204,
                        tIndexes: [913543204],
                        aoiCode: 'qwertyuiopasdfgh',
                        dsId: 'Landsat_OLI',
                        url: `http://localhost:8082/tile/Landsat_OLI/{z}/{x}/{y}.png?tIndex=913543204&bands=4,3,2&vmin=0&vmax=20000&aoi_code=qwertyuiopasdfgh`,
                        name: 'Data Layer',
                        sortOrder: 0,
                        showLegend: false,
                        showInLayerList: true
                    }
                    */
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