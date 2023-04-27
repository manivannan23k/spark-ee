import React from "react";
import { connect, useDispatch } from 'react-redux';
import Paper from '@mui/material/Paper';
import FormControlLabel from '@mui/material/FormControlLabel';
import Checkbox from '@mui/material/Checkbox';
import { changeMapExtent, removeLayer, toggleLayerState } from '../actions/index';
import Button from "@mui/material/Button";
import Typography from "@mui/material/Typography";
import DeleteIcon from '@mui/icons-material/Delete';
import IconButton from '@mui/material/IconButton';
import Grid from "@material-ui/core/Grid";
import DataService from "../services.js/Data";

const mapStateToProps = (state) => {
    return {
        map: state.MapReducer
    }
}

const LayerListItem = (props) => {
    const dispatch = useDispatch();
    const handleLayerToggle = (e) => {
        dispatch(toggleLayerState(props.layer.id, e.target.checked));
    }

    return (
        <Paper elevation={0}>
            <Grid
                justifyContent="space-between" // Add it here :)
                container
                spacing={2}
            >
                <Grid item>
                    <FormControlLabel
                        control={<Checkbox
                            onChange={handleLayerToggle}
                            checked={props.layer.active} />}
                        label={<Typography variant={'caption'} >{props.layer.name}</Typography>}
                    />
                </Grid>
                <Grid item>
                    {/* {
                        props.layer.type!=='WMS' || !props.layer.showLegend ?'':(
                            <div style={{
                                paddingTop: 12
                            }}>
                                <img src={props.layer.url + '?REQUEST=GetLegendGraphic&VERSION=1.0.0&FORMAT=image/png&WIDTH=20&HEIGHT=20&LAYER=' + props.layer.id}  alt={'legend'}/>
                            </div>
                        )
                    } */}
                    <Button onClick={() => {
                        //changeMapView
                        DataService.getAoiByCode(props.layer.aoiCode)
                            .then(r => {
                                let extent = JSON.parse(r.data.aoi_extent);
                                dispatch(changeMapExtent([
                                    [extent.coordinates[0][0][1], extent.coordinates[0][0][0]],
                                    [extent.coordinates[0][2][1], extent.coordinates[0][2][0]]
                                ]))
                            });
                    }}>
                        Z
                    </Button>
                    <Button onClick={() => {
                        dispatch(removeLayer(props.layer.id))
                    }}>
                        X
                    </Button>
                </Grid>
            </Grid>

        </Paper>
    )
}

export default connect(mapStateToProps)(LayerListItem);
