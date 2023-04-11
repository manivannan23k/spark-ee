import React, { useRef, useState, useEffect, useCallback } from "react";
import DraggableComponent from "./DraggableComponent";
import AppModal from "./AppModal";
import { connect, useDispatch } from "react-redux";
import { toggleModelBuilderDialog } from "../actions";

const mapStateToProps = (state) => {
    return {
        map: state.MapReducer,
        dialog: state.DialogReducer
    }
}

const ModelBuilder = (props) => {
    const dispatch = useDispatch()
    /***
     * Block Types:
     * 1) Raster Input - Single band, Multi band, Multi Temporal
     * 2) Raster Operations -
     *  Local: Raster Calc, Focal: Conv. Filters
     *  Temporal: Avg, SavGol
     * 3) Output - 
     */

    const datasets = [
        {
            id: 'landsat_8',
            dataType: 'int16',
            noOfBands: 7,
            name: "Landsat 8",
            description: "OLI Landsat 8",
            defaultColorScheme: {
                type: "stretched",
                colorRamp: [
                    "#000000",
                    "#ffffff"
                ]
            },
            bandMeta: [
                {
                    name: "Band 1",
                    description: "Coastal Aerosol",
                    min: 2,
                    max: 1000
                },
                {
                    name: "Band 2",
                    description: "Blue",
                    min: 100,
                    max: 1200
                },
                {
                    name: "Band 3",
                    description: "Green",
                    min: 300,
                    max: 1600
                },
                {
                    name: "Band 4",
                    description: "Red",
                    min: 0,
                    max: 2600
                },
                {
                    name: "Band 5",
                    description: "NIR",
                    min: 100,
                    max: 4500
                },
                {
                    name: "Band 6",
                    description: "SWIR1",
                    min: -200,
                    max: 4100
                },
                {
                    name: "Band 7",
                    description: "SWIR2",
                    min: -200,
                    max: 4200
                }
            ]
        }
    ]

    const [components, setComponents] = useState([])

    const addComponent = (c) => {
        setComponents([
            ...components,
            {
                type: c.type,
                name: c.name
            }
        ])
    }
    
    return <AppModal btnText={"Open Model Builder"} flag={props.dialog.showModelBuilderDialog} setFlag={(f)=>{
        dispatch(toggleModelBuilderDialog(f))
    }} content=
        <div style={{  }}>
        
            <ul style={{
                listStyle: 'none',
                margin: 0,
                padding: 0
            }}>
                {
                    [
                        {
                            type: 'input',
                            components: [
                                {
                                    name: "Raster Layer",
                                    type: "in_raster_layer"
                                },
                                {
                                    name: "Raster Band",
                                    type: "in_raster_band"
                                }
                            ]
                        },
                        {
                            type: 'operation',
                            components: [
                                {
                                    name: "Local Average",
                                    type: "op_local_avg"
                                }
                            ]
                        },
                        {
                            type: 'output',
                            components: [
                                {
                                    name: "Output Layer",
                                    type: "out_raster_layer"
                                },
                                {
                                    name: "Output Band",
                                    type: "out_raster_band"
                                }
                            ]
                        },

                    ].map(e => {
                        return <li>
                            {e.type}
                            <ul>
                                {
                                    e.components.map(c => {
                                        return <li style={{
                                            cursor: 'pointer'
                                        }} onClick={() => { addComponent(c) }}>{c.name}</li>
                                    })
                                }
                            </ul>
                        </li>
                    })
                }
            </ul>
            {
                components.map((c, _i) => {
                    return <DraggableComponent key={_i} {...c} />
                })
            }
        </div>
        
    />
}

export default connect(mapStateToProps)(ModelBuilder);