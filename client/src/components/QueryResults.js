import { connect, useDispatch } from "react-redux";
import AppModal from "./AppModal";
import { addLayer, toggleQueryResultsDialog } from "../actions";
import { useEffect, useState } from "react";
import { Button, Checkbox, Typography } from "@material-ui/core";

const mapStateToProps = (state) => {
    return {
        map: state.MapReducer,
        dialog: state.DialogReducer
    }
}

const QueryResults = (props) => {
    const dispatch = useDispatch();
    let [timeIndexes, setTimeIndexes] = useState([]);
    useEffect(() => {
        setTimeIndexes([])
    }, [props.map.queryResults])
    if (props.map.queryResults.length <= 0) {
        return <></>;
    }
    let firstResult = props.map.queryResults[0];

    return <>
        <AppModal btnText={""} flag={props.dialog.showQueryResultsDialog} setFlag={(flag) => {
            dispatch(toggleQueryResultsDialog(flag))
        }}
            content=
            <div >
                <Typography variant="h6" gutterBottom component="div">
                    Add Layer - Query Results
                </Typography>
                <ul style={{ listStyle: 'none', height: 300, overflowY: 'auto' }}>
                    {
                        props.map.queryResults.map(r => {
                            return <li><Checkbox checked={timeIndexes.indexOf(r.tIndex) > -1} onClick={(e) => {
                                if (!e.target.checked) {
                                    let idx = timeIndexes.indexOf(r.tIndex);
                                    if (idx > -1) {
                                        let ntIndexes = [...timeIndexes];
                                        ntIndexes.splice(idx, 1)
                                        setTimeIndexes(ntIndexes)
                                    }
                                } else {
                                    setTimeIndexes([
                                        ...timeIndexes,
                                        r.tIndex
                                    ])
                                }
                            }} />{r.dsName} - {new Date(r.ts).toLocaleString('in')}</li>
                        })
                    }
                </ul>
                <Button variant="contained" style={{ backgroundColor: "#6ccd6c", float: 'right' }} onClick={() => {
                    let lId = (Math.random() + 1).toString(36).substring(6);
                    // console.log(timeIndexes)
                    let layer = {
                        type: 'DATA_TILE',
                        group: 'RASTER_DATA',
                        id: lId,
                        active: true,
                        tIndex: firstResult.tIndex,
                        tIndexes: timeIndexes,//props.map.queryResults.map(qr => qr.tIndex),
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
                }}>Add</Button>
            </div>
        />

    </>
}

export default connect(mapStateToProps)(QueryResults);