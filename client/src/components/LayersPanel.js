import React from "react";
import LayerList from '../components/LayerList';
import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import QueryPanel from "./QueryPanel";

const LayersPanel = (props) => {
    return <Paper elevation={2} style={{padding: 15, maxHeight: 'calc(100vh - 120px)', overflowY: 'auto'}}>
        {/* <Typography variant="h6" gutterBottom component="div">
            Layers
        </Typography> */}
        {/* <LayerList /> */}
        <QueryPanel />
    </Paper>
}

export default LayersPanel;