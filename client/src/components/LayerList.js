import React from "react";
import {connect} from 'react-redux';
import LayerListItem from './LayerListItem';

const mapStateToProps = (state) => {
    return {
        map: state.MapReducer
    }
}

const LayerList = (props) => {

    let layers = Object.keys(props.map.layers).map(layerId=>props.map.layers[layerId]);
    layers = layers.sort((a, b)=>{ return a.sortOrder - b.sortOrder });

    return <div>
        {
            layers.map(layer=>{
                return layer.showInLayerList?(
                    <LayerListItem layer={props.map.layers[layer.id]} key={layer.id}/>
                ):''
            })
        }
    </div>
}

export default connect(mapStateToProps)(LayerList);