
import * as go from 'gojs';
import { ReactDiagram } from 'gojs-react';
import { useEffect, useRef, useState } from 'react';
import TextEditorSelectBox from './TextEditorSelectBox';
import { connect } from 'react-redux';
import TextEditorSelectBoxLayer from './TextEditorSelectBoxLayer';


const mapStateToProps = (state) => {
    return {
        map: state.MapReducer,
        dialog: state.DialogReducer
    }
}

/**
 * Diagram initialization method, which is passed to the ReactDiagram component.
 * This method is responsible for making the diagram and initializing the model and any templates.
 * The model's data should not be set here, as the ReactDiagram component handles that via the other props.
 */
function initDiagram() {
    const $ = go.GraphObject.make;
    const templateMap = new go.Map();
    const nodeTemplateInputBand =
        $(
            go.Node, "Position", { width: 200, height: 100 },
            new go.Binding('location', 'loc', go.Point.parse).makeTwoWay(go.Point.stringify),
            getNodeShape($),
            // $(go.Shape, 'RoundedRectangle', { width: 200, height: 100, position: new go.Point(0, 0), name: 'SHAPE', fill: 'white', strokeWidth: 0 }, new go.Binding('fill', 'color1')),
            getTitleText($, "top"),
            // $(go.TextBlock, { position: new go.Point(0, 0), margin: 0, font: 'bold 14pt serif', textAlign: 'center', width: 200 }, new go.Binding('text').makeTwoWay()),
            $(go.TextBlock, { position: new go.Point(0, 30), margin: 10, text: "Select Layer: ", font: '12px "Roboto", sans-serif', stroke: "black", editable: false, width: 200 }),
            $(go.TextBlock, { position: new go.Point(80, 30), margin: 10, stroke: "black", font: 'bold 12px "Roboto", sans-serif', editable: true, width: 200 }, new go.Binding('choices', 'lchoices'), new go.Binding('textEdited', 'layerEdited'), new go.Binding('text', 'defaultLayer')),
            $(go.TextBlock, { position: new go.Point(0, 60), margin: 10, text: "Select Band: ", font: '12px "Roboto", sans-serif', stroke: "black", editable: false, width: 200 }),
            $(go.TextBlock, { position: new go.Point(80, 60), margin: 10, stroke: "black", font: 'bold 12px "Roboto", sans-serif', editable: true, width: 200 }, new go.Binding('choices', 'bchoices'), new go.Binding('textEdited', 'bandEdited'), new go.Binding('text', 'defaultBand')),
            makePort("InBand", 190, 10, true, false, [5, 90], 1, 0, $),
            // makePort("L", go.Spot.Left, go.Spot.LeftSide, true, true, $),
            // makePort("R", go.Spot.Right, go.Spot.RightSide, true, true, $),
            // makePort("B", go.Spot.Bottom, go.Spot.BottomSide, true, false, $)
        )
    const nodeTemplateInputLayer =
        $(
            go.Node, "Position", { width: 200, height: 100 },
            new go.Binding('location', 'loc', go.Point.parse).makeTwoWay(go.Point.stringify),
            getNodeShape($),
            // $(go.Shape, 'RoundedRectangle', { width: 200, height: 100, position: new go.Point(0, 0), name: 'SHAPE', fill: 'white', strokeWidth: 0 }, new go.Binding('fill', 'color1')),
            getTitleText($, "top"),
            // $(go.TextBlock, { position: new go.Point(0, 0), margin: 0, font: 'bold 14pt serif', textAlign: 'center', width: 200 }, new go.Binding('text').makeTwoWay()),
            $(go.TextBlock, { position: new go.Point(0, 30), margin: 10, text: "Select Layer: ", font: '12px "Roboto", sans-serif', stroke: "black", editable: false, width: 200 }),
            $(go.TextBlock, { position: new go.Point(80, 30), margin: 10, stroke: "black", font: '12px "Roboto", sans-serif', editable: true, width: 200 }, new go.Binding('choices', 'lchoices'), new go.Binding('textEdited', 'layerEdited'), new go.Binding('text', 'defaultLayer')),
            // $(go.TextBlock, { position: new go.Point(0, 60), margin: 10, text: "Select Band: ", stroke: "red", editable: false, width: 200 }),
            // $(go.TextBlock, { position: new go.Point(80, 60), margin: 10, stroke: "red", editable: true, width: 200 }, new go.Binding('choices', 'bchoices'), new go.Binding('textEdited', 'bandEdited'), new go.Binding('text', 'defaultBand')),
            makePort("InLayer", 190, 10, true, false, [5, 90], 1, 0, $),
            // makePort("L", go.Spot.Left, go.Spot.LeftSide, true, true, $),
            // makePort("R", go.Spot.Right, go.Spot.RightSide, true, true, $),
            // makePort("B", go.Spot.Bottom, go.Spot.BottomSide, true, false, $)
        )
    const nodeTemplateOpNDI =
        $(
            go.Node, "Position", { width: 200, height: 100 },
            // new go.Binding('location', 'loc', go.Point.parse).makeTwoWay(go.Point.stringify),
            getNodeShape($),
            // $(go.Shape, 'RoundedRectangle', { width: 200, height: 100, position: new go.Point(0, 0), name: 'SHAPE', fill: 'white', strokeWidth: 0 }, new go.Binding('fill', 'color1')),
            getTitleText($),
            // $(go.TextBlock, { position: new go.Point(0, 30), margin: 10, font: 'bold 14pt serif', textAlign: 'center', width: 200 }, new go.Binding('text').makeTwoWay()),
            // $(go.TextBlock, {position: new go.Point(0, 50), margin: 10, stroke: "red", editable: false, width: 200 }, new go.Binding("text")),
            makePort("OpNDI", 190, 10, false, true, [5, 0], 0, 1, $),
            makePort("OpNDI", 190, 10, true, false, [5, 90], 1, 0, $),
        );
    const nodeTemplateOpLocalAvg =
        $(
            go.Node, "Position", { width: 200, height: 100 },
            // new go.Binding('location', 'loc', go.Point.parse).makeTwoWay(go.Point.stringify),
            getNodeShape($),
            // $(go.Shape, 'RoundedRectangle', { width: 200, height: 100, position: new go.Point(0, 0), name: 'SHAPE', fill: 'white', strokeWidth: 0 }, new go.Binding('fill', 'color1')),
            getTitleText($),
            // $(go.TextBlock, { position: new go.Point(0, 30), margin: 10, font: 'bold 14pt serif', textAlign: 'center', width: 200 }, new go.Binding('text').makeTwoWay()),
            // $(go.TextBlock, {position: new go.Point(0, 50), margin: 10, stroke: "red", editable: false, width: 200 }, new go.Binding("text")),
            makePort("OpLocalAvg", 190, 10, false, true, [5, 0], 0, 1, $),
            makePort("OpLocalAvg", 190, 10, true, false, [5, 90], 1, 0, $),
        );
    const nodeTemplateOpSavGol =
        $(
            go.Node, "Position", { width: 200, height: 100 },
            // new go.Binding('location', 'loc', go.Point.parse).makeTwoWay(go.Point.stringify),
            getNodeShape($),
            // $(go.Shape, 'RoundedRectangle', { width: 200, height: 100, position: new go.Point(0, 0), name: 'SHAPE', fill: 'white', strokeWidth: 0 }, new go.Binding('fill', 'color1')),
            getTitleText($),
            // $(go.TextBlock, { position: new go.Point(0, 30), margin: 10, font: 'bold 14pt serif', textAlign: 'center', width: 200 }, new go.Binding('text').makeTwoWay()),
            // $(go.TextBlock, {position: new go.Point(0, 50), margin: 10, stroke: "red", editable: false, width: 200 }, new go.Binding("text")),
            makePort("OpSavGol", 190, 10, false, true, [5, 0], 0, 1, $),
            makePort("OpSavGol", 190, 10, true, false, [5, 90], 1, 0, $),
        );
    const nodeTemplateOutBand =
        $(
            go.Node, "Position", {
            width: 200, height: 100
        },
            new go.Binding('location', 'loc', go.Point.parse).makeTwoWay(go.Point.stringify),
            // $(go.Shape, 'RoundedRectangle', { width: 205, height: 105, position: new go.Point(0, 0), name: 'SHAPE', strokeWidth: 0, fill: "rgba(0,0,0,0.2)" }),
            getNodeShape($),
            // $(go.Shape, 'RoundedRectangle', { width: 200, height: 100, position: new go.Point(0, 0), name: 'SHAPE', fill: 'white', strokeWidth: 0 }, new go.Binding('fill', 'color1')),
            getTitleText($),
            // $(go.TextBlock, { position: new go.Point(0, 30), margin: 10, font: 'bold 14pt serif', textAlign: 'center', width: 200, text: "Output R.Band" }),
            makePort("OutBand", 190, 10, false, true, [5, 0], 0, 1, $),
        );
    const nodeTemplateOutLayer =
        $(
            go.Node, "Position", { width: 200, height: 100 },
            new go.Binding('location', 'loc', go.Point.parse).makeTwoWay(go.Point.stringify),
            getNodeShape($),
            getTitleText($),
            // $(go.TextBlock, { position: new go.Point(0, 30), margin: 10, font: 'bold 14pt serif', textAlign: 'center', width: 200, text: "Output R.Layer" }),
            makePort("OutBand", 190, 10, false, true, [5, 0], 0, 1, $),
        );

    templateMap.add('', nodeTemplateInputBand);
    templateMap.add('opNDI', nodeTemplateOpNDI);
    templateMap.add('inLayer', nodeTemplateInputLayer);
    templateMap.add('outRasterband', nodeTemplateOutBand)
    templateMap.add('opLocalAvg', nodeTemplateOpLocalAvg)
    templateMap.add('outRasterlayer', nodeTemplateOutLayer)
    templateMap.add('opSavGol', nodeTemplateOpSavGol)

    const diagram =
        $(go.Diagram,
            {
                "textEditingTool.defaultTextEditor": TextEditorSelectBox,
                'undoManager.isEnabled': true,  // must be set to allow for model change listening
                // 'undoManager.maxHistoryLength': 0,  // uncomment disable undo/redo functionality
                'clickCreatingTool.archetypeNodeData': { text: 'new node', color: 'lightblue' },
                model: new go.GraphLinksModel(
                    {
                        linkKeyProperty: 'key'
                    })
            });

    diagram.nodeTemplateMap = templateMap

    diagram.addDiagramListener("TextEdited", e => {
        // changeEvt(e)
    });
    diagram.linkTemplate = $(go.Link,
        {
            curve: go.Link.Bezier,
            toEndSegmentLength: 30, fromEndSegmentLength: 30
        },
        $(go.Shape, { strokeWidth: 1.5 }),
        $(go.Shape,
            { toArrow: "standard", strokeWidth: 0, fill: "black" }),
    );
    // diagram.linkTemplate =
    //     $(go.Link,  // the whole link panel
    //         {
    //             routing: go.Link.AvoidsNodes,
    //             curve: go.Link.JumpOver,
    //             corner: 5, toShortLength: 4,
    //             relinkableFrom: true,
    //             relinkableTo: true,
    //             reshapable: true,
    //             resegmentable: true,
    //             // mouse-overs subtly highlight links:
    //             mouseEnter: (e, link) => link.findObject("HIGHLIGHT").stroke = "rgba(30,144,255,0.2)",
    //             mouseLeave: (e, link) => link.findObject("HIGHLIGHT").stroke = "transparent",
    //             selectionAdorned: false
    //         },
    //         new go.Binding("points").makeTwoWay(),
    //         $(go.Shape,  // the highlight shape, normally transparent
    //             { isPanelMain: true, strokeWidth: 8, stroke: "transparent", name: "HIGHLIGHT" }),
    //         $(go.Shape,  // the link path shape
    //             { isPanelMain: true, stroke: "dodgerblue", strokeWidth: 2 },
    //             new go.Binding("stroke", "dodgerblue").ofObject()),
    //         $(go.Shape,  // the arrowhead
    //             { toArrow: "standard", strokeWidth: 0, fill: "dodgerblue" }),

    //     );

    return diagram;
}

function getNodeShape($) {
    return $(go.Shape, 'RoundedRectangle', { width: 200, height: 100, position: new go.Point(0, 0), name: 'SHAPE', fill: '#F1F5EE', strokeWidth: 0 }, new go.Binding('fill', 'color1'))
}

function getTitleText($, loc) {
    if (loc === 'top') {
        return $(go.TextBlock, { position: new go.Point(0, 0), margin: 10, font: '12px "Roboto", sans-serif', textAlign: 'center', width: 200 }, new go.Binding('text').makeTwoWay())
    }
    return $(go.TextBlock, { position: new go.Point(0, 30), margin: 10, font: '12px "Roboto", sans-serif', textAlign: 'center', width: 200 }, new go.Binding('text').makeTwoWay())
}


function makePort(name, width, height, output, input, position, fromMaxLinks, toMaxLinks, $) {
    // var horizontal = align.equals(go.Spot.Top) || align.equals(go.Spot.Bottom);
    // the port is basically just a transparent rectangle that stretches along the side of the node,
    // and becomes colored when the mouse passes over it
    return $(go.Shape,
        {
            fill: "#AA6F6F",  // changed to a color in the mouseEnter event handler
            strokeWidth: 0,  // no stroke
            width: width,//horizontal ? NaN : 8,  // if not stretching horizontally, just 8 wide
            height: height,//!horizontal ? NaN : 8,  // if not stretching vertically, just 8 tall
            // alignment: align,  // align the port on the main Shape
            // stretch: (horizontal ? go.GraphObject.Horizontal : go.GraphObject.Vertical),
            portId: name,  // declare this object to be a "port"
            // fromSpot: spot,  // declare where links may connect at this port
            fromLinkable: output,  // declare whether the user may draw links from here
            // toSpot: spot,  // declare where links may connect at this port
            toLinkable: input,  // declare whether the user may draw links to here
            cursor: "pointer",  // show a different cursor to indicate potential link point
            mouseEnter: (e, port) => {  // the PORT argument will be this Shape
                if (!e.diagram.isReadOnly) port.fill = "rgba(255,0,255,0.5)";
            },
            mouseLeave: (e, port) => port.fill = "#AA6F6F", //"transparent",
            position: new go.Point(...position),
            toMaxLinks: toMaxLinks, fromMaxLinks: fromMaxLinks,
        });
}

const GoDiagram = (props) => {

    const layers = Object.keys(props.map.layers).filter(lk => {
        return props.map.layers[lk].type === 'DATA_TILE'
    }).map(k => props.map.layers[k]);

    function handleModelChange(changes) {
        props.modelChange(changes)
    }

    const diagramRef = useRef()
    const [nodeArray, setNodeArray] = useState([
        // { key: 0, text: 'In Raster Band', color1: 'white', loc: '0 0', defaultBand: 'Band 1',  choices: ['Band 1', 'Band 2', 'Band 3', 'Band 4'] },
        // { key: 1, text: 'In Raster Band', color1: 'white', loc: '250 0', defaultBand: 'Band 1',  choices: ['Band 1', 'Band 2', 'Band 3', 'Band 4'] },
        // { key: 2, text: 'Algorithm', color1: 'white', loc: '100 150', defaultBand: 'NDVI', category: 'opNDI' },
        // { key: 3, text: 'Out Raster Band', color1: 'white', loc: '100 300', defaultBand: 'Band 1',  choices: ['Band 1', 'Band 2', 'Band 3', 'Band 4'] },
        // { key: 1, text: 'Beta', color: 'orange', loc: '150 0' },
        // { key: 2, text: 'Gamma', color: 'lightgreen', loc: '0 150' },
        // { key: 3, text: 'Delta', color: 'pink', loc: '150 150' }
    ])

    useEffect(() => {
        if (props.components) {
            // console.log(props.components)
            let nodes = [];
            let components = [...props.components.inputs, ...props.components.operations]
            if (props.components.output) {
                components.push(props.components.output)
            }
            for (let i = 0; i < components.length; i++) {
                let node = null;
                const component = components[i];
                switch (component.type) {
                    case "in_raster_band":
                        node = {
                            key: component.componentId,
                            text: component.name,
                            color1: 'rgb(151 255 178)',
                            // loc: component.loc,
                            defaultBand: component.band ? `Band ${component.band}` : "-",
                            bchoices: component.id ? Array(component.noOfBands).fill(0).map((e, i) => `Band ${i + 1}`) : [],
                            lchoices: layers.map(l => l.id),
                            defaultLayer: component.id ? `Layer: ${component.id}` : "-",
                            bandEdited: (e) => {
                                handleModelChange({
                                    nodeId: component.componentId, value: e.text, eventType: "nodeUpdate", type: component.type + '#' + 'Band', nodeType: "inputs"
                                })
                            },
                            layerEdited: (e) => {
                                handleModelChange({
                                    nodeId: component.componentId, value: e.text, eventType: "nodeUpdate", type: component.type + '#' + 'Layer', nodeType: "inputs"
                                })
                            }
                        }
                        break;

                    case "in_raster_layer":
                        node = {
                            key: component.componentId,
                            text: component.name,
                            color1: 'rgb(151 255 178)',
                            bchoices: component.id ? Array(component.noOfBands).fill(0).map((e, i) => `Band ${i + 1}`) : [],
                            lchoices: layers.map(l => l.id),
                            defaultLayer: component.id,
                            layerEdited: (e) => {
                                handleModelChange({
                                    nodeId: component.componentId, value: e.text, eventType: "nodeUpdate", type: component.type + '#' + 'Layer', nodeType: "inputs"
                                })
                            },
                            category: 'inLayer'
                        }
                        console.log("RLayer", component.id)
                        break;
                    // case "out_raster_band":
                    //     node = { key: i, text: component.name, color1: 'white', loc: '0 0', category: 'outRasterband'}
                    //     break;
                    case "op_ndi":
                        node = {
                            key: component.componentId,
                            text: component.name,
                            color1: 'rgb(255 251 133)',
                            category: 'opNDI'
                        }
                    case "op_local_avg":
                        node = {
                            key: component.componentId,
                            text: component.name,
                            color1: 'rgb(255 251 133)',
                            category: 'opLocalAvg'
                        }
                        break;
                    case "op_local_avg":
                        node = {
                            key: component.componentId,
                            text: component.name,
                            color1: 'rgb(255 251 133)',
                            category: 'opLocalAvg'
                        }
                        break;
                    case "op_savgol":
                        node = {
                            key: component.componentId,
                            text: component.name,
                            color1: 'rgb(255 251 133)',
                            category: 'opSavGol'
                        }
                        break;
                    case "out_raster_band":
                        node = {
                            key: component.componentId,
                            text: component.name,
                            color1: 'rgb(142 224 255)',
                            category: 'outRasterband'
                        }
                        break;
                    case "out_raster_layer":
                        node = {
                            key: component.componentId,
                            text: component.name,
                            color1: 'rgb(142 224 255)',
                            category: 'outRasterlayer'
                        }
                        break;
                    default:
                        // node = { key: i, text: 'Algorithm', color1: 'white', loc: '100 150', defaultBand: 'NDVI', category: 'opNDI' }
                        break;
                }
                // console.log(component.componentId, component.type, node)
                if (node)
                    nodes.push(node)
            }
            // console.log(nodes)
            // console.log(nodes)
            setNodeArray(nodes);
        }
    }, [props.components])


    return <div style={{ width: '100%', height: '100%', backgroundColor: '#999' }}>
        <ReactDiagram style={{ width: '100%', height: '100%', backgroundColor: '#999' }}
            ref={diagramRef}
            initDiagram={initDiagram}
            divClassName='diagram-component'
            nodeDataArray={nodeArray}
            linkDataArray={props.modelLinks}
            onModelChange={handleModelChange}

        />
    </div>
}


export default connect(mapStateToProps)(GoDiagram);