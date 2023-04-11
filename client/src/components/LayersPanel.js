import React from "react";
import LayerList from '../components/LayerList';
import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import QueryPanel from "./QueryPanel";
import AppModal from "./AppModal";
import { connect, useDispatch } from "react-redux";
import { toggleAddLayerDialog } from "../actions";
import ModelBuilder from "./ModelBuilder";

const mapStateToProps = (state) => {
    return {
        map: state.MapReducer,
        dialog: state.DialogReducer
    }
}

const LayersPanel = (props) => {
    
    const dispatch = useDispatch();

    return <Paper elevation={2} style={{padding: 15, maxHeight: 'calc(100vh - 120px)', overflowY: 'auto'}}>
        {/* <Typography variant="h6" gutterBottom component="div">
            Layers
        </Typography> */}
        {/* <LayerList /> */}
        <AppModal btnText={"Add Layer"} flag={props.dialog.showAddLayerDialog} setFlag={(flag)=>{
            dispatch(toggleAddLayerDialog(flag))
        }} content=<QueryPanel /> />
        <ModelBuilder />
    </Paper>
}

export default connect(mapStateToProps)(LayersPanel);