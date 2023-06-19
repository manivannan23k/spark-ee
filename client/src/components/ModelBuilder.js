import React, { useRef, useState, useEffect, useCallback } from "react";
import DraggableComponent from "./DraggableComponent";
import AppModal from "./AppModal";
import { connect, useDispatch } from "react-redux";
import { toggleModelBuilderDialog } from "../actions";
import GoDiagram from "./GoDiagram";
import Config from '../config.js';
import { Button, TextField, Typography } from "@material-ui/core";

const mapStateToProps = (state) => {
    return {
        map: state.MapReducer,
        dialog: state.DialogReducer
    }
}

const ModelBuilder = (props) => {
    const dispatch = useDispatch()

    const [processResult, setProcessResult] = useState("")
    const [processName, setProcessName] = useState("");
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

    let inputTypes = ['in_raster_band', 'in_raster_layer'];
    let outputTypes = ['out_raster_band', 'out_raster_layer'];
    let operationTypes = ['op_ndi', 'op_local_avg', 'op_savgol', 'op_fpca', 'op_mosaic', 'op_local_dif', 'op_bandsel', 'op_tstomb'];
    const [components, setComponents] = useState({
        inputs: [],
        output: null,
        operations: []
    })
    const [modelLinks, setModelLinks] = useState([]);

    const getRandomString = (l) => (Math.random() + 1).toString(36).substring(l);

    const addComponent = (componentType, c) => {
        if (componentType === 'input') {
            let component = {
                "componentId": getRandomString(6),
                "id": "",
                "tIndexes": [],
                "isTemporal": false,
                "aoiCode": "",
                "dsName": "",
                "layerName": "",
                "type": c.type,
                "name": c.name,
                "noOfBands": 7,
                "band": "",
                "loc": "0 0",
                "prevLoc": null
            }
            setComponents({
                ...components,
                inputs: [
                    ...components.inputs,
                    component
                ]
            })
        } else if (componentType === 'operation') {
            let component = {
                "componentId": getRandomString(6),
                "id": "",
                "type": c.type,
                "name": c.name,
                "inputs": [],
                "output": {
                    "id": "O1"
                },
                "params": "None"
            }
            setComponents({
                ...components,
                operations: [
                    ...components.operations,
                    component
                ]
            })
        } else if (componentType === 'output') {
            let component = {
                "componentId": getRandomString(6),
                "type": c.type,
                "id": "",
                "name": c.name
            }
            setComponents({
                ...components,
                output: component
            })
        }
        // setComponents([
        //     ...components,
        //     {
        //         id: components.length,
        //         type: c.type,
        //         name: c.name
        //     }
        // ])
    }

    // useEffect(()=>{
    //     console.log(components)
    // }, [components])

    useEffect(() => {
        // console.log(components)
    }, [components])

    return <AppModal btnText={"Open Model Builder"} flag={props.dialog.showModelBuilderDialog} setFlag={(f) => {
        dispatch(toggleModelBuilderDialog(f))
    }} content=
        <div>
            <Typography variant="h6" style={{ textAlign: 'center' }}>
                Model Builder
            </Typography>
            <br />
            <div style={{ display: 'flex' }}>
                <div style={{ width: '20%', display: 'inline-block' }}>
                    <ul style={{
                        listStyle: 'none',
                        margin: 0,
                        padding: 0
                    }}>
                        {
                            [
                                {
                                    type: 'input',
                                    name: "Input Types",
                                    components: [
                                        {
                                            name: "Raster Layer",
                                            type: "in_raster_layer"
                                        },
                                        // {
                                        //     name: "Raster Band",
                                        //     type: "in_raster_band"
                                        // }
                                    ]
                                },
                                {
                                    type: 'operation',
                                    name: "Operations",
                                    components: [
                                        {
                                            name: "Normalized Difference",
                                            type: "op_ndi"
                                        },
                                        {
                                            name: "Local Average",
                                            type: "op_local_avg"
                                        },
                                        {
                                            name: "Local Difference",
                                            type: "op_local_dif"
                                        },
                                        {
                                            name: "SavGol Filter",
                                            type: "op_savgol"
                                        },
                                        {
                                            name: "FPCA",
                                            type: "op_fpca"
                                        },
                                        {
                                            name: "Mosaic",
                                            type: "op_mosaic"
                                        },
                                        {
                                            name: "Band Selector",
                                            type: "op_bandsel"
                                        },
                                        {
                                            name: "Time Series -> Bands",
                                            type: "op_tstomb"
                                        }
                                    ]
                                },
                                {
                                    type: 'output',
                                    name: "Output Types",
                                    components: [
                                        {
                                            name: "Output Layer",
                                            type: "out_raster_layer"
                                        },
                                        // {
                                        //     name: "Output Band",
                                        //     type: "out_raster_band"
                                        // }
                                    ]
                                },

                            ].map(e => {
                                return <li style={{
                                    margin: 6,
                                    background: '#eee',
                                    padding: 8,
                                    width: 'fit-content',
                                    borderRadius: 4,
                                    boxShadow: '2px 2px 2px -1px #7A7A79',
                                    fontFamily: 'fangsong',
                                    fontSize: '16',
                                    fontWeight: 500
                                }}>
                                    {e.name}
                                    <ul style={{ listStyle: 'none', padding: 0 }}>
                                        {
                                            e.components.map(c => {
                                                return <li style={{
                                                    width: '150px',
                                                    cursor: 'pointer',
                                                    padding: '4px',
                                                    textDecoration: 'none',
                                                    background: e.type === 'input' ? 'rgb(151 255 178)' : (e.type === 'output' ? 'rgb(142 224 255)' : 'rgb(255 251 133)'),
                                                    borderRadius: '4px',
                                                    boxShadow: '2px 2px 2px -1px #7A7A79',
                                                    textAlign: 'center',
                                                    margin: '2px',
                                                    fontFamily: 'fangsong',
                                                    fontSize: '14px',
                                                    fontWeight: 300
                                                }} onClick={() => { addComponent(e.type, c) }}>{c.name}</li>
                                            })
                                        }
                                    </ul>
                                </li>
                            })
                        }
                    </ul>
                </div>
                {/* {
                components.map((c, _i) => {
                    return <DraggableComponent key={_i} {...c} />
                })
            } */}
                <div style={{ width: '80%', display: 'inline-block', height: 'calc(100vh - 400px)', maxHeight: 600, overflowY: 'auto' }}>
                    <GoDiagram components={components} modelLinks={modelLinks} modelChange={(changes) => {
                        // console.log("Model updated", changes)
                        // if(changes.insertedLinkKeys){
                        //     //new link
                        //     console.log("Link added")
                        // }else if(changes.modifiedLinkData){
                        //     //modified link
                        //     console.log("Link updated")
                        // }else if(changes.removedLinkKeys){
                        //     console.log("Link removed")
                        // }

                        try {
                            if (changes.eventType === 'nodeUpdate') {
                                let component = components[changes.nodeType].findIndex((c) => c.componentId === changes.nodeId);
                                if (inputTypes.indexOf(changes.type.split('#')[0]) !== -1) {
                                    let inputs = [...components.inputs];
                                    inputs.splice(component, 1);
                                    component = components[changes.nodeType][component];
                                    let layer = null;
                                    switch (changes.type) {
                                        case "in_raster_band#Layer":
                                            layer = props.map.layers[changes.value]
                                            component.tIndexes = layer.tIndexes;
                                            component.aoiCode = layer.aoiCode;
                                            component.isTemporal = layer.tIndexes.length > 1;
                                            component.dsName = layer.dsId;
                                            component.id = changes.value
                                            component.loc = '100 100'
                                            break;

                                        case "in_raster_band#Band":
                                            layer = props.map.layers[component.id]
                                            component.band = changes.value.split(" ")[1]
                                            component.loc = '200 150'
                                            break;

                                        case "in_raster_layer#Layer":
                                            layer = props.map.layers[changes.value]
                                            component.tIndexes = layer.tIndexes;
                                            component.aoiCode = layer.aoiCode;
                                            component.isTemporal = layer.tIndexes.length > 1;
                                            component.dsName = layer.dsId;
                                            component.id = changes.value
                                            component.loc = '100 100'
                                            break;
                                    }
                                    setComponents({
                                        ...components,
                                        inputs: [
                                            ...inputs,
                                            component
                                        ]
                                    })
                                }


                                if (operationTypes.indexOf(changes.type.split('#')[0]) !== -1) {
                                    let operations = [...components.operations];
                                    operations.splice(component, 1);
                                    component = components[changes.nodeType][component];
                                    let a, b, c;
                                    switch (changes.type) {
                                        case "op_ndi#Band1":
                                            a = changes.value.split(' ')[1]; //layerId
                                            a = [...components.inputs, ...components.operations.map(ope => {
                                                return {
                                                    componentId: ope.output.layer,
                                                    noOfBands: ope.noOfBands,
                                                    id: ope.componentId
                                                }
                                            })].filter(e => e.id === a.slice(0, a.length - 1))[0].componentId;
                                            b = changes.value.split('_')[1]; //bandId
                                            if (!component.params || component.params === "None") {
                                                component.params = `${a + ":" + b}#null`
                                            }
                                            if (component.params && component.params.indexOf('#') > -1) {
                                                component.params = `${a + ":" + b}#` + component.params.split('#')[1]
                                            }
                                            component.b1 = changes.value;
                                            component.noOfBands = 1;
                                            break;

                                        case "op_ndi#Band2":
                                            a = changes.value.split(' ')[1]; //layerId
                                            a = [...components.inputs, ...components.operations.map(ope => {
                                                return {
                                                    componentId: ope.output.layer,
                                                    noOfBands: ope.noOfBands,
                                                    id: ope.componentId
                                                }
                                            })].filter(e => e.id === a.slice(0, a.length - 1))[0].componentId;
                                            b = changes.value.split('_')[1]; //bandId
                                            if (!component.params || component.params === "None") {
                                                component.params = `null#${a + ":" + b}`
                                            }
                                            if (component.params && component.params.indexOf('#') > -1) {
                                                component.params = component.params.split('#')[0] + `#${a + ":" + b}`
                                            }
                                            component.b2 = changes.value;
                                            component.noOfBands = 1;
                                            break;

                                        case "op_savgol#Power":
                                            if (!component.params || component.params === "None") {
                                                component.params = `${changes.value}#null`
                                            }
                                            if (component.params && component.params.indexOf('#') > -1) {
                                                component.params = `${changes.value}#` + component.params.split('#')[1]
                                            }
                                            component.power = changes.value;
                                            break;

                                        case "op_savgol#Window":
                                            if (!component.params || component.params === "None") {
                                                component.params = `null#${changes.value}`
                                            }
                                            if (component.params && component.params.indexOf('#') > -1) {
                                                component.params = component.params.split('#')[0] + `#${changes.value}`
                                            }
                                            component.windowSize = changes.value;
                                            break;


                                        case "op_mosaic#startDate":


                                            if (component.params === "None") {
                                                component.params = `${changes.value}#null#null#null`
                                            }
                                            a = component.params.split("#")
                                            a[0] = new Date(changes.value).getTime();
                                            component.params = a.join("#");
                                            component.startDate = changes.value;
                                            try {
                                                component.noOfBands = [...components.inputs, ...components.operations.map(ope => {
                                                    return {
                                                        componentId: ope.output.layer,
                                                        noOfBands: ope.noOfBands,
                                                        id: ope.componentId
                                                    }
                                                })].filter(e => {
                                                    if (component.inputs.map(ei => ei.layer).indexOf(e.componentId) > -1) {
                                                        return true;
                                                    }
                                                    return false;
                                                })[0].noOfBands
                                            } catch (e) {
                                                component.noOfBands = 1;
                                            }
                                            break;


                                        case "op_mosaic#endDate":
                                            if (component.params === "None") {
                                                component.params = `null#${changes.value}#null#null`
                                            }
                                            a = component.params.split("#")
                                            a[1] = new Date(changes.value).getTime();
                                            component.params = a.join("#");
                                            component.endDate = changes.value;
                                            try {
                                                component.noOfBands = [...components.inputs, ...components.operations.map(ope => {
                                                    return {
                                                        componentId: ope.output.layer,
                                                        noOfBands: ope.noOfBands,
                                                        id: ope.componentId
                                                    }
                                                })].filter(e => {
                                                    if (component.inputs.map(ei => ei.layer).indexOf(e.componentId) > -1) {
                                                        return true;
                                                    }
                                                    return false;
                                                })[0].noOfBands
                                            } catch (e) {
                                                component.noOfBands = 1;
                                            }
                                            break;

                                        case "op_mosaic#intervalValue":
                                            if (component.params === "None") {
                                                component.params = `null#null#${changes.value}#null`
                                            }
                                            a = component.params.split("#")
                                            a[2] = changes.value;
                                            component.params = a.join("#");
                                            component.intervalValue = changes.value;
                                            try {
                                                component.noOfBands = [...components.inputs, ...components.operations.map(ope => {
                                                    return {
                                                        componentId: ope.output.layer,
                                                        noOfBands: ope.noOfBands,
                                                        id: ope.componentId
                                                    }
                                                })].filter(e => {
                                                    if (component.inputs.map(ei => ei.layer).indexOf(e.componentId) > -1) {
                                                        return true;
                                                    }
                                                    return false;
                                                })[0].noOfBands
                                            } catch (e) {
                                                component.noOfBands = 1;
                                            }
                                            break;

                                        case "op_mosaic#intervalUnit":

                                            if (component.params === "None") {
                                                component.params = `null#null#null#${changes.value}`
                                            }
                                            a = component.params.split("#")
                                            a[3] = changes.value;
                                            component.params = a.join("#");
                                            component.intervalUnit = changes.value;
                                            try {
                                                component.noOfBands = [...components.inputs, ...components.operations.map(ope => {
                                                    return {
                                                        componentId: ope.output.layer,
                                                        noOfBands: ope.noOfBands,
                                                        id: ope.componentId
                                                    }
                                                })].filter(e => {
                                                    if (component.inputs.map(ei => ei.layer).indexOf(e.componentId) > -1) {
                                                        return true;
                                                    }
                                                    return false;
                                                })[0].noOfBands
                                            } catch (e) {
                                                component.noOfBands = 1;
                                            }
                                            break;

                                        case "op_bandsel#Band1":
                                            a = changes.value.split(' ')[1]; //layerId
                                            a = [...components.inputs, ...components.operations.map(ope => {
                                                return {
                                                    componentId: ope.output.layer,
                                                    noOfBands: ope.noOfBands,
                                                    id: ope.componentId
                                                }
                                            })].filter(e => e.id === a.slice(0, a.length - 1))[0].componentId;
                                            b = changes.value.split('_')[1]; //bandId
                                            component.params = `${a + ":" + b}`
                                            component.b1 = changes.value;
                                            component.noOfBands = 1;
                                            break;
                                    }
                                    setComponents({
                                        ...components,
                                        operations: [
                                            ...operations,
                                            component
                                        ]
                                    })
                                }

                            }

                            // if(changes.modifiedNodeData){
                            //     let inputs = [...components["inputs"]];
                            //     let upComponents = changes.modifiedNodeData.map(n=>{
                            //         let componentIdx = inputs.findIndex((c)=>c.componentId===n.key);
                            //         let component = inputs[componentIdx];
                            //         if(component.loc!==n.loc && n.loc!==component.prevLoc){
                            //             inputs.splice(componentIdx, 1);
                            //             component.loc = n.loc;
                            //             return component
                            //         }else{
                            //             return null;
                            //         }
                            //     }).filter(e=>Boolean(e))
                            //     // console.log(upComponents)
                            //     if(upComponents.length>0){
                            //         // setComponents({
                            //         //     ...components,
                            //         //     inputs: [
                            //         //         ...inputs,
                            //         //         ...upComponents
                            //         //     ]
                            //         // })
                            //     }
                            // }
                            if (changes.removedNodeKeys) {
                                let inputs = [...components["inputs"]];
                                let operations = [...components["operations"]];
                                // console.log(inputs)
                                changes.removedNodeKeys.map(n => {
                                    let componentIdx = inputs.findIndex((c) => c.componentId === n);
                                    if (componentIdx !== -1) {
                                        inputs.splice(componentIdx, 1);
                                    }
                                    componentIdx = operations.findIndex((c) => c.componentId === n);
                                    if (componentIdx !== -1) {
                                        operations.splice(componentIdx, 1);
                                    }
                                });
                                setComponents({
                                    ...components,
                                    inputs: [
                                        ...inputs
                                    ],
                                    operations: [
                                        ...operations
                                    ]
                                })
                            }

                            if (changes.insertedLinkKeys) {

                                const getCompById = (id) => {
                                    let inputs = [...components["inputs"]];
                                    let operations = [...components["operations"]];

                                    let componentIdx = inputs.findIndex((c) => c.componentId === id);
                                    if (componentIdx !== -1) {
                                        return { component: inputs[componentIdx], type: "inputs", index: componentIdx }
                                    }
                                    componentIdx = operations.findIndex((c) => c.componentId === id);
                                    if (componentIdx !== -1) {
                                        return { component: operations[componentIdx], type: "operations", index: componentIdx }
                                    }
                                    if (components.output && components.output.componentId === id) {
                                        return { component: components.output, type: "operations", index: componentIdx }
                                    }
                                    return null;
                                }

                                let linkIds = changes.insertedLinkKeys;
                                let newLink = changes.modifiedLinkData[0];
                                if (modelLinks.map(e => e.key).indexOf(newLink.key) !== -1) {
                                    // return console.log("Link Exists")
                                }
                                let fromComp = getCompById(newLink.from);
                                let toComp = getCompById(newLink.to);
                                if (inputTypes.indexOf(fromComp.component.type) !== -1 && operationTypes.indexOf(toComp.component.type) !== -1) {
                                    let opInput = {
                                        layer: fromComp.component.componentId,
                                        band: fromComp.band
                                    };
                                    toComp.component.inputs.push(opInput)
                                    let operations = [...components.operations];
                                    operations.splice(toComp.index, 1);
                                    setComponents({
                                        ...components,
                                        operations: [
                                            ...operations,
                                            toComp.component
                                        ]
                                    })
                                    setModelLinks([
                                        ...modelLinks,
                                        newLink
                                    ])
                                } else if (operationTypes.indexOf(fromComp.component.type) !== -1 && outputTypes.indexOf(toComp.component.type) !== -1) {
                                    let modelOutput = { ...components.output };
                                    // modelOutput.id = fromComp.component.componentId
                                    let opOutput = {
                                        layer: modelOutput.componentId,
                                        band: fromComp.band
                                    };
                                    fromComp.component.output = opOutput
                                    let operations = [...components.operations];
                                    operations.splice(fromComp.index, 1);
                                    setComponents({
                                        ...components,
                                        operations: [
                                            ...operations,
                                            fromComp.component
                                        ],
                                        output: modelOutput
                                    })
                                    setModelLinks([
                                        ...modelLinks,
                                        newLink
                                    ])
                                } else if (operationTypes.indexOf(fromComp.component.type) !== -1 && operationTypes.indexOf(toComp.component.type) !== -1) {
                                    console.log(fromComp.component, toComp.component);
                                    if (!fromComp.component.output.layer || (components.output && components.output.componentId === fromComp.component.output.layer))
                                        fromComp.component.output.layer = getRandomString(6);
                                    let opInput = {
                                        layer: fromComp.component.output.layer,
                                        band: fromComp.component.output.band
                                    };
                                    toComp.component.inputs.push(opInput)
                                    let operations = [...components.operations];
                                    operations.splice(toComp.index, 1);
                                    setComponents({
                                        ...components,
                                        operations: [
                                            ...operations,
                                            toComp.component
                                        ]
                                    })
                                    setModelLinks([
                                        ...modelLinks,
                                        newLink
                                    ])


                                }
                            }
                        } catch (e) {
                            console.log(e);
                        }
                    }} />
                    <div>
                        <TextField
                            label="Process name"
                            value={processName}
                            onChange={(e) => {
                                setProcessName(e.target.value)
                            }}
                        />
                    </div>
                    <div style={{ textAlign: 'right' }}>
                        <Button onClick={() => {
                            setComponents({
                                inputs: [],
                                output: null,
                                operations: []
                            })
                            setModelLinks([])
                            setProcessName("")
                            setProcessResult("")
                        }}>Clear</Button>
                        <Button onClick={() => {
                            // console.log(components);
                            let reqComps = {
                                inputs: [], operations: [], output: {}
                            };
                            for (let i = 0; i < components.inputs.length; i++) {
                                let input = components.inputs[i];
                                input = {
                                    "id": input.componentId,
                                    "tIndexes": input.tIndexes,
                                    "isTemporal": input.isTemporal,
                                    "aoiCode": input.aoiCode,
                                    "dsName": input.dsName
                                }
                                reqComps.inputs.push(input)
                            }

                            for (let i = 0; i < components.operations.length; i++) {
                                let operation = components.operations[i];
                                operation = {
                                    "id": operation.componentId,
                                    "type": operation.type,
                                    "inputs": operation.inputs.map(inp => {
                                        let il = components.inputs.filter(e => { return e.componentId === inp.layer })[0]
                                        if (!il) {
                                            //from operation
                                            return {
                                                id: inp.layer,
                                                band: 0
                                            }
                                        }
                                        return {
                                            id: inp.layer,
                                            band: il.band ? parseInt(il.band) : 0
                                        }
                                    }),
                                    "output": {
                                        "id": operation.output.layer
                                    },
                                    "params": operation.params
                                }
                                reqComps.operations.push(operation)
                            }
                            reqComps.output = {
                                id: components.output.componentId
                            }

                            console.log(reqComps)
                            fetch(`${Config.PROCESS_HOST}/process/submit`, {
                                body: JSON.stringify({
                                    data: JSON.stringify(reqComps),
                                    name: processName || "Sample Process"
                                }),
                                method: 'POST',
                                headers: {
                                    'Accept': 'application/json',
                                    'Content-Type': 'application/json'
                                }
                            })
                                .then(r => r.json())
                                .then(r => { console.log(r); setProcessResult(r.data) })
                                .catch(e => console.log(e))


                        }}>Run</Button>
                    </div>
                    <div>{processResult}</div>
                </div>
            </div>
        </div>

    />
}

export default connect(mapStateToProps)(ModelBuilder);